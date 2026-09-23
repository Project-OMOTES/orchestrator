"""Delete worker resources declared on Prefect flow runs."""

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

import psycopg
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from minio import Minio
from omotes_sdk.prefect_util import MinioResource, TimeseriesResource
from psycopg import sql

from orchestrator.settings import Settings

logger = logging.getLogger("orchestrator")


class ResourceCleanupError(RuntimeError):
    """Raised when a declared job resource cannot be safely removed."""


class ResourceCleanupConfigurationError(ResourceCleanupError):
    """Raised when credentials for a declared job resource are unavailable."""


class ResourceCleanupUnavailableError(ResourceCleanupError):
    """Raised when a cleanup endpoint cannot be reached or authenticated."""


class ResourceCleanupNotFoundError(ResourceCleanupError):
    """Raised when a declared cleanup resource is already absent."""


class CleanupIssueKind(StrEnum):
    """Classification for a cleanup issue."""

    CONNECTION_FAILED = "connection_failed"
    DATA_NOT_FOUND = "data_not_found"
    DELETE_FAILED = "delete_failed"


@dataclass(frozen=True)
class CleanupResourceIssue:
    """Identify a cleanup issue and the resource it affects."""

    resource_type: str
    host: str
    port: int
    kind: CleanupIssueKind
    detail: str

    def __str__(self) -> str:
        """Return a log-friendly description of the affected resource."""
        return f"type={self.resource_type} host={self.host} port={self.port} kind={self.kind} detail={self.detail}"


class ResourceCleanupBatchError(ResourceCleanupError):
    """Raised after all resources were attempted and one or more failed."""

    def __init__(self, failures: list[CleanupResourceIssue], not_found: list[CleanupResourceIssue]) -> None:
        """Store failed and already-absent cleanup resources."""
        self.failures = failures
        self.not_found = not_found
        super().__init__("One or more job cleanup resources could not be deleted")


def cleanup_resources(resources: Iterable[MinioResource | TimeseriesResource], settings: Settings) -> None:
    """Delete all declared resources using credentials for their exact endpoint."""
    failures: list[CleanupResourceIssue] = []
    not_found: list[CleanupResourceIssue] = []
    for resource in resources:
        try:
            if isinstance(resource, MinioResource):
                _delete_minio_resource(resource, settings)
            elif resource.type == "influxdb":
                _delete_influxdb_resource(resource, settings)
            else:
                _delete_postgresql_resource(resource, settings)
        except ResourceCleanupNotFoundError as exc:
            issue = _resource_issue(resource, str(exc), CleanupIssueKind.DATA_NOT_FOUND)
            not_found.append(issue)
            logger.warning("Cleanup resource data not found %s", issue)
        except ResourceCleanupConfigurationError as exc:
            failures.append(_resource_issue(resource, str(exc), CleanupIssueKind.CONNECTION_FAILED))
            logger.error("Cleanup resource credentials unavailable %s", failures[-1])
        except ResourceCleanupUnavailableError as exc:
            failures.append(_resource_issue(resource, str(exc), CleanupIssueKind.CONNECTION_FAILED))
            logger.exception("Could not connect to job cleanup resource %s", failures[-1])
        except ResourceCleanupError as exc:
            failures.append(_resource_issue(resource, str(exc), CleanupIssueKind.DELETE_FAILED))
            logger.error("Failed to delete job cleanup resource %s", failures[-1])
        except (psycopg.OperationalError, InfluxDBClientError) as exc:
            failure = _resource_issue(resource, str(exc), CleanupIssueKind.CONNECTION_FAILED)
            failures.append(failure)
            logger.exception("Could not connect to job cleanup resource %s", failure)
        except Exception as exc:
            failure = _resource_issue(resource, str(exc), CleanupIssueKind.DELETE_FAILED)
            failures.append(failure)
            logger.exception("Failed to delete job cleanup resource %s", failure)

    if failures or not_found:
        raise ResourceCleanupBatchError(failures, not_found)


def _resource_issue(
    resource: MinioResource | TimeseriesResource,
    detail: str,
    kind: CleanupIssueKind = CleanupIssueKind.DELETE_FAILED,
) -> CleanupResourceIssue:
    return CleanupResourceIssue(resource.type, resource.host, resource.port, kind, detail)


def _delete_minio_resource(resource: MinioResource, settings: Settings) -> None:
    if resource.host != settings.minio_host or resource.port != int(settings.minio_port):
        _raise_missing_credentials("MinIO", resource.host, resource.port)
    run_folder = resource.path.removeprefix("flow-results/").strip("/")
    if not resource.path.startswith("flow-results/") or not run_folder or "/" in run_folder:
        raise ResourceCleanupError(f"Refusing to delete MinIO path outside one flow-results folder: {resource.path}")

    client = Minio(
        f"{resource.host}:{resource.port}",
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret,
        secure=False,
    )
    object_count = 0
    for object_info in client.list_objects(resource.bucket, prefix=f"flow-results/{run_folder}/", recursive=True):
        object_count += 1
        client.remove_object(resource.bucket, object_info.object_name)
    if object_count == 0:
        raise ResourceCleanupNotFoundError(f"bucket={resource.bucket} path={resource.path}")


def _delete_influxdb_resource(resource: TimeseriesResource, settings: Settings) -> None:
    if not _credentials_match(
        resource.host,
        resource.port,
        settings.influx_host,
        settings.influx_port,
        settings.influx_username,
        settings.influx_password,
    ):
        _raise_missing_credentials("InfluxDB", resource.host, resource.port)
    _require_uuid_identifier("InfluxDB database", resource.database)

    client = InfluxDBClient(
        host=resource.host,
        port=resource.port,
        username=settings.influx_username,
        password=settings.influx_password,
    )
    if any(database["name"] == resource.database for database in client.get_list_database()):
        client.drop_database(resource.database)
    else:
        raise ResourceCleanupNotFoundError(f"database={resource.database}")


def _delete_postgresql_resource(resource: TimeseriesResource, settings: Settings) -> None:
    if resource.schema_name is None:
        raise ResourceCleanupError("PostgreSQL cleanup resource has no schema")
    if not _credentials_match(
        resource.host,
        resource.port,
        settings.postgres_host,
        settings.postgres_port,
        settings.postgres_username,
        settings.postgres_password,
    ):
        _raise_missing_credentials("PostgreSQL", resource.host, resource.port)
    _require_uuid_identifier("PostgreSQL schema", resource.schema_name)

    with (
        psycopg.connect(
            host=resource.host,
            port=resource.port,
            dbname=resource.database,
            user=settings.postgres_username,
            password=settings.postgres_password,
            autocommit=True,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (resource.schema_name,),
        )
        if cursor.fetchone() is None:
            raise ResourceCleanupNotFoundError(f"database={resource.database} schema={resource.schema_name}")
        cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(resource.schema_name)))


def _credentials_match(
    resource_host: str,
    resource_port: int,
    configured_host: str | None,
    configured_port: int | None,
    username: str | None,
    password: str | None,
) -> bool:
    return (
        resource_host == configured_host
        and resource_port == configured_port
        and username is not None
        and password is not None
    )


def _raise_missing_credentials(resource_type: str, host: str, port: int) -> None:
    logger.error("No %s credentials configured for cleanup resource host=%s port=%s", resource_type, host, port)
    raise ResourceCleanupConfigurationError(f"No {resource_type} credentials configured for {host}:{port}")


def _require_uuid_identifier(resource_type: str, identifier: str) -> None:
    try:
        UUID(identifier)
    except ValueError as exc:
        raise ResourceCleanupError(
            f"Refusing to delete {resource_type} with non-UUID identifier {identifier!r}"
        ) from exc
