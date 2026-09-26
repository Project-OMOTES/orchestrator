"""Delete worker resources declared on Prefect flow runs."""

import logging
from collections.abc import Iterable
from uuid import UUID

import psycopg
from influxdb import InfluxDBClient
from minio import Minio
from omotes_sdk.prefect_util import MinioResource, TimeseriesResource
from psycopg import sql

from orchestrator.settings import Settings

logger = logging.getLogger("orchestrator")


def cleanup_resources(resources: Iterable[MinioResource | TimeseriesResource], settings: Settings) -> None:
    """Delete all declared resources using credentials for their exact endpoint, logging failures."""
    for resource in resources:
        try:
            if isinstance(resource, MinioResource):
                _delete_minio_resource(resource, settings)
            elif resource.type == "influxdb":
                _delete_influxdb_resource(resource, settings)
            else:
                _delete_postgresql_resource(resource, settings)
        except Exception:
            logger.exception(
                "Failed to delete job cleanup resource type=%s host=%s port=%s",
                resource.type,
                resource.host,
                resource.port,
            )


def _delete_minio_resource(resource: MinioResource, settings: Settings) -> None:
    if resource.host != settings.minio_host or resource.port != int(settings.minio_port):
        _raise_missing_credentials("MinIO", resource.host, resource.port)
    run_folder = resource.path.removeprefix("flow-results/").strip("/")
    if not resource.path.startswith("flow-results/") or not run_folder or "/" in run_folder:
        raise ValueError(f"Refusing to delete MinIO path outside one flow-results folder: {resource.path}")

    client = Minio(
        f"{resource.host}:{resource.port}",
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret,
        secure=False,
    )
    deleted_count = 0
    for object_info in client.list_objects(resource.bucket, prefix=f"flow-results/{run_folder}/", recursive=True):
        client.remove_object(resource.bucket, object_info.object_name)
        deleted_count += 1
    if deleted_count == 0:
        logger.warning(
            "Cleanup resource data not found type=minio bucket=%s resource_path=%s",
            resource.bucket,
            resource.path,
        )


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
        logger.warning("Cleanup resource data not found type=influxdb database=%s", resource.database)


def _delete_postgresql_resource(resource: TimeseriesResource, settings: Settings) -> None:
    if resource.schema_name is None:
        raise ValueError("PostgreSQL cleanup resource has no schema")
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
            logger.warning("Cleanup resource data not found type=postgresql schema=%s", resource.schema_name)
            return

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
    raise ValueError(f"No {resource_type} credentials configured for cleanup resource host={host} port={port}")


def _require_uuid_identifier(resource_type: str, identifier: str) -> None:
    try:
        UUID(identifier)
    except ValueError as exc:
        raise ValueError(f"Refusing to delete {resource_type} with non-UUID identifier {identifier!r}") from exc
