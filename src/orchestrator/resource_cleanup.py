"""Delete worker resources declared on Prefect flow runs."""

import logging
from collections.abc import Iterable
from urllib.parse import quote
from urllib.request import Request, urlopen
from uuid import UUID

import boto3
import psycopg
from botocore.client import BaseClient
from botocore.config import Config
from influxdb import InfluxDBClient
from omotes_sdk.prefect_util import S3Resource, TimeseriesResource
from psycopg import sql

from orchestrator.settings import Settings

logger = logging.getLogger("orchestrator")


def s3_client(settings: Settings) -> BaseClient:
    """Connect to the S3 gateway using path-style bucket URLs."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{settings.s3_host}:{settings.s3_port}",
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(s3={"addressing_style": "path"}),
    )


def cleanup_resources(resources: Iterable[S3Resource | TimeseriesResource], settings: Settings) -> bool:
    """Delete all declared resources using credentials for their exact endpoint, logging failures."""
    succeeded = True
    for resource in resources:
        try:
            if isinstance(resource, S3Resource):
                _delete_s3_resource(resource, settings)
            elif resource.type == "influxdb":
                _delete_influxdb_resource(resource, settings)
            else:
                _delete_postgresql_resource(resource, settings)
        except Exception:
            succeeded = False
            logger.exception(
                "Failed to delete job cleanup resource type=%s host=%s port=%s",
                resource.type,
                resource.host,
                resource.port,
            )
    return succeeded


def _delete_s3_resource(resource: S3Resource, settings: Settings) -> None:
    run_folder = _s3_run_folder(resource, settings)

    client = s3_client(settings)
    deleted_count = 0
    pages = client.get_paginator("list_objects_v2").paginate(
        Bucket=resource.bucket, Prefix=f"flow-results/{run_folder}/"
    )
    for page in pages:
        for object_info in page.get("Contents", []):
            client.delete_object(Bucket=resource.bucket, Key=object_info["Key"])
            deleted_count += 1
    remove_s3_folder_if_empty(resource, settings)
    if deleted_count == 0:
        logger.warning(
            "Cleanup resource data not found type=s3 bucket=%s resource_path=%s",
            resource.bucket,
            resource.path,
        )


def remove_s3_folder_if_empty(resource: S3Resource, settings: Settings) -> None:
    """Remove a SeaweedFS directory entry only if it has no remaining children."""
    run_folder = _s3_run_folder(resource, settings)
    filer_path = f"/buckets/{quote(resource.bucket, safe='')}/flow-results/{quote(run_folder, safe='')}"
    request = Request(
        f"http://{settings.s3_host}:{settings.s3_filer_port}{filer_path}?recursive=false",
        method="DELETE",
    )
    with urlopen(  # noqa: S310 - the URL uses the configured SeaweedFS Filer host over HTTP
        request, timeout=settings.request_timeout_seconds
    ):
        pass


def _s3_run_folder(resource: S3Resource, settings: Settings) -> str:
    if resource.host != settings.s3_host or resource.port != int(settings.s3_port):
        _raise_missing_credentials("S3", resource.host, resource.port)
    run_folder = resource.path.removeprefix("flow-results/").strip("/")
    if (
        not resource.path.startswith("flow-results/")
        or not run_folder
        or "/" in run_folder
        or run_folder in {".", ".."}
    ):
        raise ValueError(f"Refusing to delete S3 path outside one flow-results folder: {resource.path}")
    return run_folder


def _delete_influxdb_resource(resource: TimeseriesResource, settings: Settings) -> None:
    _require_uuid_identifier("InfluxDB database", resource.database)
    if not _credentials_match(
        resource.host,
        resource.port,
        settings.influx_host,
        settings.influx_port,
        settings.influx_username,
        settings.influx_password,
    ):
        _raise_missing_credentials("InfluxDB", resource.host, resource.port)

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
    _require_uuid_identifier("PostgreSQL schema", resource.schema_name)
    if not _credentials_match(
        resource.host,
        resource.port,
        settings.postgres_host,
        settings.postgres_port,
        settings.postgres_username,
        settings.postgres_password,
    ):
        _raise_missing_credentials("PostgreSQL", resource.host, resource.port)

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
