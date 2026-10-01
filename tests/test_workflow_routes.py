import asyncio
import base64
import json
import logging
import re
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import cast
from uuid import UUID, uuid4

import httpx
import pytest
from conftest import InMemoryJobStore
from fastapi.testclient import TestClient
from omotes_sdk import prefect_util
from omotes_sdk.job_status import JobStatus
from omotes_sdk.prefect_util import MinioResource, TimeseriesResource
from prefect.exceptions import ObjectNotFound
from prefect.states import Cancelled, Running, StateType

import orchestrator.main as app_main
from orchestrator import resource_cleanup, workflow_registry
from orchestrator.database import Job, JobCleanupResource
from orchestrator.main import create_app
from orchestrator.routes import job as job_routes
from orchestrator.settings import Settings, settings
from orchestrator.workflow_types import WorkflowDefinition


async def _fake_get_flow_versions_by_name(flow_names: list[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    if "grow_optimizer" in flow_names:
        result["grow_optimizer"] = ["0.10.1", "0.10.2"]
    if "simulator" in flow_names:
        result["simulator"] = ["latest"]
    return result


def _persist_job(job_store: InMemoryJobStore, job_id: UUID, job_name: str = "test-job") -> None:
    job_store.jobs[job_id] = Job(
        job_id=job_id,
        job_name=job_name,
        workflow_type="test-workflow",
        workflow_version=None,
        user_name="test-user",
        status=JobStatus.ENQUEUED,
    )


def test_workflow_upload_replaces_in_memory_list(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """Verify uploading a workflow replaces the in-memory registry."""
    workflow_registry._workflows = []
    monkeypatch.setattr("orchestrator.workflow_registry.get_flow_versions_by_name", _fake_get_flow_versions_by_name)

    first_payload = [
        {
            "workflow_type_name": "grow_optimizer_default",
            "workflow_type_description_name": "Draft Design - Optimization",
            "prefect_flow_name": "grow_optimizer",
        }
    ]
    second_payload = [
        {
            "workflow_type_name": "simulator",
            "workflow_type_description_name": "Conceptual Design - Simulation",
            "prefect_flow_name": "simulator",
            "workflow_parameters": {
                "timestep": {
                    "type": "integer",
                    "default": 3600,
                    "minimum": 0,
                }
            },
        }
    ]

    response = client.get("/workflow/")

    assert response.status_code == 200
    assert response.json() == []

    response = client.post(
        "/workflow/",
        json=first_payload,
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "grow_optimizer_default",
            "description": "Draft Design - Optimization",
            "versions": ["0.10.1", "0.10.2"],
        }
    ]

    response = client.post(
        "/workflow/",
        json=second_payload,
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "simulator",
            "description": "Conceptual Design - Simulation",
            "versions": ["latest"],
            "schema": {
                "type": "object",
                "properties": {
                    "timestep": {
                        "type": "integer",
                        "default": 3600,
                        "minimum": 0,
                    }
                },
                "required": ["timestep"],
            },
            "uischema": {
                "type": "VerticalLayout",
                "elements": [
                    {
                        "type": "Control",
                        "scope": "#/properties/timestep",
                    }
                ],
            },
        }
    ]

    response = client.get("/workflow/")

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "simulator",
            "description": "Conceptual Design - Simulation",
            "versions": ["latest"],
            "schema": {
                "type": "object",
                "properties": {
                    "timestep": {
                        "type": "integer",
                        "default": 3600,
                        "minimum": 0,
                    }
                },
                "required": ["timestep"],
            },
            "uischema": {
                "type": "VerticalLayout",
                "elements": [
                    {
                        "type": "Control",
                        "scope": "#/properties/timestep",
                    }
                ],
            },
        }
    ]


def test_workflow_upload_rejects_invalid_json_schema_properties(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """Reject workflow parameters that are not valid JSON Schema properties."""
    workflow_registry._workflows = []
    monkeypatch.setattr("orchestrator.workflow_registry.get_flow_versions_by_name", _fake_get_flow_versions_by_name)

    payload = [
        {
            "workflow_type_name": "simulator",
            "workflow_type_description_name": "Conceptual Design - Simulation",
            "prefect_flow_name": "simulator",
            "workflow_parameters": {
                "timestep": {
                    "type": "duration",
                    "default": 3600,
                }
            },
        }
    ]

    response = client.post("/workflow/", json=payload)

    assert response.status_code == 422


def test_workflow_settings_file_is_loaded_at_startup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Load workflow definitions from the configured file during startup."""
    workflow_registry._workflows = []
    monkeypatch.setattr("orchestrator.workflow_registry.get_flow_versions_by_name", _fake_get_flow_versions_by_name)
    workflow_file = tmp_path / "workflows.json"
    workflow_file.write_text(
        json.dumps(
            [
                {
                    "workflow_type_name": "grow_optimizer_no_heat_losses",
                    "workflow_type_description_name": "Draft Design - Quickscan Validation",
                    "prefect_flow_name": "grow_optimizer",
                    "prefect_flow_version": "0.10.2",
                    "versions": ["0.10.1", "0.10.2"],
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(app_main.settings, "workflow_settings_file", str(workflow_file))

    with TestClient(create_app()) as startup_client:
        response = startup_client.get("/workflow/")

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "grow_optimizer_no_heat_losses",
            "description": "Draft Design - Quickscan Validation",
            "versions": ["0.10.1", "0.10.2"],
        }
    ]


def test_get_job_returns_not_found_when_prefect_flow_run_was_deleted(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """Reject unknown durable jobs without querying Prefect."""
    job_id = uuid4()

    monkeypatch.setattr(
        job_routes,
        "get_flow_run_status_and_results",
        lambda *_args, **_kwargs: pytest.fail("must not query Prefect"),
    )

    response = client.get(f"/job/{job_id}")

    assert response.status_code == 404
    assert response.json() == {"detail": f"Unknown job {job_id}"}


def test_list_jobs_does_not_import_prefect_only_runs(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, job_store: InMemoryJobStore
) -> None:
    """Ignore Prefect runs that are not owned by the durable job store."""
    prefect_only_job_id = uuid4()

    async def get_prefect_only_run() -> list[SimpleNamespace]:
        return [SimpleNamespace(id=prefect_only_job_id, name="external", tags=[], state=Running())]

    monkeypatch.setattr(job_routes, "get_runs", get_prefect_only_run)

    response = client.get("/job/")

    assert response.status_code == 200
    assert response.json() == []
    assert prefect_only_job_id not in job_store.jobs


def test_delete_job_logs_flow_run_metadata(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    job_store: InMemoryJobStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log the deleted job's name and identifying tags."""
    job_id = uuid4()
    _persist_job(job_store, job_id, "asset-constraints")

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, flow_run_id: UUID) -> SimpleNamespace:
            assert flow_run_id == job_id
            return SimpleNamespace(
                name="asset-constraints",
                tags=["type:grow_optimizer_default", "user:john"],
                state=Cancelled(),
            )

    async def delete_existing_flow_run(flow_run_id: UUID) -> bool:
        assert flow_run_id == job_id
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    with caplog.at_level(logging.INFO, logger="orchestrator"):
        response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert response.json() == {"job_id": str(job_id), "deleted": True}
    assert (
        "delete_job status=DELETED job_name=asset-constraints workflow_type=grow_optimizer_default user_name=john"
        in caplog.messages
    )


def test_delete_job_returns_false_when_flow_run_was_already_deleted(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    job_store: InMemoryJobStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Report a concurrent flow run deletion without raising an exception."""
    job_id = uuid4()
    _persist_job(job_store, job_id, "finished-job")

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, flow_run_id: UUID) -> SimpleNamespace:
            assert flow_run_id == job_id
            return SimpleNamespace(name="finished-job", tags=[], state=Cancelled())

    async def delete_missing_flow_run(flow_run_id: UUID) -> bool:
        assert flow_run_id == job_id
        return False

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "delete_run", delete_missing_flow_run)

    with caplog.at_level(logging.ERROR, logger="orchestrator"):
        response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert response.json() == {"job_id": str(job_id), "deleted": False}
    assert f"delete_job failed to delete job_id={job_id}: flow run was not found" in caplog.messages


def test_delete_job_requests_cancellation_for_running_flow(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, job_store: InMemoryJobStore
) -> None:
    """Return after requesting cancellation and finish deletion after Prefect confirms it."""
    job_id = uuid4()
    _persist_job(job_store, job_id, "active-job")

    class FakeClientContext:
        def __init__(self) -> None:
            self.cancelled_flow_run_id: UUID | None = None
            self.read_count = 0

        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, flow_run_id: UUID) -> SimpleNamespace:
            assert flow_run_id == job_id
            self.read_count += 1
            return SimpleNamespace(name="active-job", tags=[], state=Running() if self.read_count == 1 else Cancelled())

        async def set_flow_run_state(self, flow_run_id: UUID, state: object) -> SimpleNamespace:
            assert getattr(state, "type", None) == StateType.CANCELLING
            self.cancelled_flow_run_id = flow_run_id
            return SimpleNamespace(status=job_routes.SetStateStatus.ACCEPT)

    async def delete_existing_flow_run(_: UUID) -> bool:
        return True

    fake_client = FakeClientContext()
    monkeypatch.setattr(job_routes, "get_client", lambda: fake_client)
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    response = client.delete(f"/job/{job_id}")

    assert response.status_code == 202
    assert response.json() == {"job_id": str(job_id), "deleted": False}
    assert fake_client.cancelled_flow_run_id == job_id


def test_delete_job_finishes_cleanup_when_prefect_history_disappears_during_cancellation(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    job_store: InMemoryJobStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Finish durable cleanup if Prefect history is manually deleted after cancellation."""
    job_id = uuid4()
    _persist_job(job_store, job_id, "active-job")
    resource = MinioResource(host="omotes-minio", port=9000, path="flow-results/run-id")
    resource_key = resource.model_dump_json(by_alias=True)
    job_store.resources[job_id] = {
        resource_key: JobCleanupResource(resource_key=resource_key, resource=resource),
    }
    cleaned_resources: list[MinioResource] = []

    class FakeClientContext:
        def __init__(self) -> None:
            self.read_count = 0

        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            self.read_count += 1
            if self.read_count == 1:
                return SimpleNamespace(name="active-job", tags=[], state=Running())
            raise ObjectNotFound(Exception("Flow run not found"))

        async def set_flow_run_state(self, _: UUID, _state: object) -> SimpleNamespace:
            return SimpleNamespace(status=job_routes.SetStateStatus.ACCEPT)

    def clean_resources(resources: list[MinioResource], _settings: object) -> bool:
        cleaned_resources.extend(resources)
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "cleanup_resources", clean_resources)
    monkeypatch.setattr(job_routes, "delete_run", lambda _: pytest.fail("Prefect history is already missing"))

    with caplog.at_level(logging.WARNING, logger="orchestrator"):
        response = client.delete(f"/job/{job_id}")

    assert response.status_code == 202
    assert response.json() == {"job_id": str(job_id), "deleted": False}
    assert cleaned_resources == [resource]
    assert job_store.jobs[job_id].deleted_at is not None
    assert job_store.resources[job_id] == {}
    assert f"delete_job job_id={job_id}: Prefect history was deleted while waiting for cancellation" in caplog.messages


async def test_wait_for_flow_run_cancellation_times_out(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Stop polling when Prefect does not produce a terminal state in time."""
    job_id = uuid4()

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            await asyncio.Future()
            raise AssertionError("unreachable")

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes.settings, "cancellation_timeout_seconds", 0.01)

    with caplog.at_level(logging.ERROR, logger="orchestrator"):
        result = await job_routes._wait_for_flow_run_cancellation(job_id)

    assert result == job_routes.CancellationWaitResult.FAILED
    assert f"delete_job cancellation timed out job_id={job_id} timeout_seconds=0.01" in caplog.messages


def test_delete_job_cleans_declared_resources_before_deleting_history(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, job_store: InMemoryJobStore
) -> None:
    """Use durable cleanup resources before deleting the Prefect flow run."""
    job_id = uuid4()
    _persist_job(job_store, job_id, "finished-job")
    cleaned_resources: list[MinioResource] = []
    delete_called = False
    resource = MinioResource(host="omotes-minio", port=9000, path="flow-results/run-id")
    resource_key = resource.model_dump_json(by_alias=True)
    job_store.resources[job_id] = {
        resource_key: JobCleanupResource(resource_key=resource_key, resource=resource),
    }

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            return SimpleNamespace(name="finished-job", tags=[], state=Cancelled())

    def clean_resources(resources: list[MinioResource], _settings: object) -> None:
        cleaned_resources.extend(resources)

    async def delete_existing_flow_run(_: UUID) -> bool:
        nonlocal delete_called
        delete_called = True
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "cleanup_resources", clean_resources)
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert delete_called
    assert cleaned_resources == [MinioResource(host="omotes-minio", port=9000, path="flow-results/run-id")]


def test_delete_job_logs_cleanup_failure_when_credentials_do_not_match(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    job_store: InMemoryJobStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log cleanup failures without failing the delete response."""
    job_id = uuid4()
    _persist_job(job_store, job_id, "finished-job")
    delete_called = False
    resource = MinioResource(host="unconfigured-minio", port=9000, path="flow-results/run-id")
    resource_key = resource.model_dump_json(by_alias=True)
    job_store.resources[job_id] = {
        resource_key: JobCleanupResource(resource_key=resource_key, resource=resource),
    }

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            return SimpleNamespace(name="finished-job", tags=[], state=Cancelled())

    async def delete_existing_flow_run(_: UUID) -> bool:
        nonlocal delete_called
        delete_called = True
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    with caplog.at_level(logging.ERROR, logger="orchestrator"):
        response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert delete_called
    assert "Failed to delete job cleanup resource type=minio host=unconfigured-minio port=9000" in caplog.messages


def test_minio_cleanup_limits_deletion_to_one_run_folder(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use a trailing slash so MinIO prefix matching cannot include sibling runs."""
    prefixes: list[str] = []

    class FakeMinio:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def list_objects(self, _bucket: str, prefix: str, recursive: bool) -> list[object]:
            assert recursive
            prefixes.append(prefix)
            return []

    monkeypatch.setattr(resource_cleanup, "Minio", FakeMinio)

    resource_cleanup._delete_minio_resource(
        MinioResource(host=settings.minio_host, port=int(settings.minio_port), path="flow-results/run-1"),
        settings,
    )

    assert prefixes == ["flow-results/run-1/"]


def test_influx_cleanup_rejects_non_uuid_database_before_connecting(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never drop an Influx database unless its name is a per-run UUID."""
    monkeypatch.setattr(resource_cleanup, "InfluxDBClient", lambda **_kwargs: pytest.fail("must not connect"))
    resource = TimeseriesResource(
        type="influxdb",
        host=settings.influx_host or "",
        port=settings.influx_port or 0,
        database="omotes_timeseries",
    )

    with pytest.raises(ValueError, match="non-UUID"):
        resource_cleanup._delete_influxdb_resource(resource, settings)


@pytest.fixture
def postgres_cleanup_settings() -> Settings:
    """Provide deterministic PostgreSQL cleanup credentials for unit tests."""
    return settings.model_copy(
        update={
            "postgres_host": "postgres.example.test",
            "postgres_port": 5432,
            "postgres_username": "test-user",
            "postgres_password": "test-password",
        }
    )


def test_postgresql_cleanup_allows_uuid_schema_in_declared_database(
    monkeypatch: pytest.MonkeyPatch, postgres_cleanup_settings: Settings
) -> None:
    """Delete UUID-named schemas without imposing a database allowlist."""
    connection_kwargs: dict[str, object] = {}

    class FakeCursor:
        def __init__(self) -> None:
            self.queries: list[object] = []

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        def execute(self, query: object, _parameters: object = None) -> None:
            self.queries.append(query)

        def fetchone(self) -> tuple[int] | None:
            return (1,)

    fake_cursor = FakeCursor()

    class FakeConnection:
        def __enter__(self) -> "FakeConnection":
            return self

        def __exit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        def cursor(self) -> FakeCursor:
            return fake_cursor

    def connect(**_kwargs: object) -> FakeConnection:
        connection_kwargs.update(_kwargs)
        return FakeConnection()

    monkeypatch.setattr(resource_cleanup.psycopg, "connect", connect)
    resource = TimeseriesResource(
        type="postgresql",
        host=postgres_cleanup_settings.postgres_host or "",
        port=postgres_cleanup_settings.postgres_port or 0,
        database="another_timeseries_database",
        schema_name=str(uuid4()),
    )

    resource_cleanup._delete_postgresql_resource(resource, postgres_cleanup_settings)

    assert connection_kwargs["dbname"] == "another_timeseries_database"
    assert len(fake_cursor.queries) == 2


def test_postgresql_cleanup_logs_missing_schema(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    postgres_cleanup_settings: Settings,
) -> None:
    """Log an already-removed PostgreSQL schema without issuing DROP SCHEMA."""
    executed_queries: list[object] = []

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        def execute(self, query: object, _parameters: object = None) -> None:
            executed_queries.append(query)

        def fetchone(self) -> tuple[int] | None:
            return None

    class FakeConnection:
        def __enter__(self) -> "FakeConnection":
            return self

        def __exit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        def cursor(self) -> FakeCursor:
            return FakeCursor()

    def connect(**_kwargs: object) -> FakeConnection:
        return FakeConnection()

    monkeypatch.setattr(resource_cleanup.psycopg, "connect", connect)
    schema_name = str(uuid4())
    resource = TimeseriesResource(
        type="postgresql",
        host=postgres_cleanup_settings.postgres_host or "",
        port=postgres_cleanup_settings.postgres_port or 0,
        database="another_timeseries_database",
        schema_name=schema_name,
    )

    with caplog.at_level(logging.INFO, logger="orchestrator"):
        resource_cleanup._delete_postgresql_resource(resource, postgres_cleanup_settings)

    assert len(executed_queries) == 1
    assert f"Cleanup resource data not found type=postgresql schema={schema_name}" in caplog.messages


def test_postgresql_cleanup_rejects_non_uuid_schema_before_connecting(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never drop a PostgreSQL schema unless its name is a per-run UUID."""
    monkeypatch.setattr(resource_cleanup.psycopg, "connect", lambda **_kwargs: pytest.fail("must not connect"))
    resource = TimeseriesResource(
        type="postgresql",
        host=settings.postgres_host or "",
        port=settings.postgres_port or 0,
        database="any_database",
        schema_name="public",
    )

    with pytest.raises(ValueError, match="non-UUID"):
        resource_cleanup._delete_postgresql_resource(resource, settings)


def test_delete_job_keeps_history_when_prefect_rejects_cancellation(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, job_store: InMemoryJobStore
) -> None:
    """Do not delete an active run when Prefect rejects its cancellation request."""
    job_id = uuid4()
    _persist_job(job_store, job_id, "active-job")
    delete_called = False

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            return SimpleNamespace(name="active-job", tags=[], state=Running())

        async def set_flow_run_state(self, _: UUID, _state: object) -> SimpleNamespace:
            return SimpleNamespace(status=job_routes.SetStateStatus.REJECT)

    async def delete_existing_flow_run(_: UUID) -> bool:
        nonlocal delete_called
        delete_called = True
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert response.json() == {"job_id": str(job_id), "deleted": False}
    assert not delete_called


def test_delete_job_continues_when_rejected_cancellation_is_already_cancelled(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, job_store: InMemoryJobStore
) -> None:
    """Use the current Prefect state when a cancellation proposal loses a race."""
    job_id = uuid4()
    _persist_job(job_store, job_id, "cancelled-job")

    class FakeClientContext:
        def __init__(self) -> None:
            self.read_count = 0

        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            self.read_count += 1
            state = Running() if self.read_count == 1 else Cancelled()
            return SimpleNamespace(name="cancelled-job", tags=[], state=state)

        async def set_flow_run_state(self, _: UUID, _state: object) -> SimpleNamespace:
            return SimpleNamespace(status=job_routes.SetStateStatus.REJECT)

    async def delete_existing_flow_run(_: UUID) -> bool:
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert response.json() == {"job_id": str(job_id), "deleted": True}
    assert job_store.jobs[job_id].deleted_at is not None


def test_delete_job_returns_false_when_prefect_metadata_read_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    job_store: InMemoryJobStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log metadata lookup connectivity failures and return false."""
    job_id = uuid4()
    _persist_job(job_store, job_id)

    class FailingClientContext:
        async def __aenter__(self) -> "FailingClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            raise httpx.ConnectError("Prefect is unavailable")

    monkeypatch.setattr(job_routes, "get_client", lambda: FailingClientContext())

    with caplog.at_level(logging.ERROR, logger="orchestrator"):
        response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert response.json() == {"job_id": str(job_id), "deleted": False}
    assert f"delete_job failed job_id={job_id}" in caplog.messages


def test_delete_job_returns_false_for_invalid_job_id(client: TestClient, caplog: pytest.LogCaptureFixture) -> None:
    """Log malformed job IDs instead of returning a validation exception."""
    with caplog.at_level(logging.ERROR, logger="orchestrator"):
        response = client.delete("/job/not-a-uuid")

    assert response.status_code == 200
    assert response.json() == {"job_id": None, "deleted": False}
    assert "delete_job failed: invalid job_id=not-a-uuid" in caplog.messages


def test_delete_job_returns_false_for_unknown_database_job(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """Reject unknown durable jobs without querying Prefect."""
    job_id = uuid4()
    monkeypatch.setattr(job_routes, "get_client", lambda: pytest.fail("must not query Prefect"))

    with caplog.at_level(logging.ERROR, logger="orchestrator"):
        response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert response.json() == {"job_id": str(job_id), "deleted": False}
    assert f"delete_job failed job_id={job_id}: job was not found in the orchestrator database" in caplog.messages


def test_delete_job_deletes_history_when_flow_run_has_no_state(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    job_store: InMemoryJobStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log a missing Prefect state and continue deleting what remains."""
    job_id = uuid4()
    _persist_job(job_store, job_id, "stateless-job")
    delete_called = False

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            return SimpleNamespace(name="stateless-job", tags=[], state=None)

    async def delete_existing_flow_run(_: UUID) -> bool:
        nonlocal delete_called
        delete_called = True
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    with caplog.at_level(logging.ERROR, logger="orchestrator"):
        response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert response.json() == {"job_id": str(job_id), "deleted": True}
    assert delete_called
    assert job_store.jobs[job_id].deleted_at is not None
    assert f"delete_job failed job_id={job_id}: flow run has no Prefect state" in caplog.messages


class _FakeClientContext:
    def __init__(self, deployments: Sequence[object]) -> None:
        self._deployments = deployments
        self.read_deployments_kwargs = None
        self.create_flow_run_kwargs = None

    async def __aenter__(self) -> "_FakeClientContext":
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        return None

    async def read_deployments(self, **kwargs: object) -> Sequence[object]:
        self.read_deployments_kwargs = kwargs
        return self._deployments

    async def create_flow_run_from_deployment(self, **kwargs: object) -> SimpleNamespace:
        self.create_flow_run_kwargs = kwargs
        return SimpleNamespace(id=uuid4())


async def test_get_flow_versions_by_name_sorted_allows_any_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Allow and correctly sort arbitrary Prefect deployment versions."""
    deployments = [
        SimpleNamespace(name="grow_optimizer:0.10.1"),
        SimpleNamespace(name="grow_optimizer:local"),
        SimpleNamespace(name="grow_optimizer:0.10.2"),
        SimpleNamespace(name="grow_optimizer:0.10.2-rc.1"),
        SimpleNamespace(name="simulator:0.9.0"),
        SimpleNamespace(name="simulator:dev"),
        SimpleNamespace(name="simulator:0.10.0-rc.1"),
        SimpleNamespace(name="simulator:0.10.0"),
        SimpleNamespace(name="simulator:latest"),
    ]
    monkeypatch.setattr(prefect_util, "get_client", lambda: _FakeClientContext(deployments))

    versions_by_name = await prefect_util.get_flow_versions_by_name(["grow_optimizer", "simulator"])

    assert versions_by_name == {
        "grow_optimizer": ["0.10.2", "0.10.2-rc.1", "0.10.1", "local"],
        "simulator": ["0.10.0", "0.10.0-rc.1", "0.9.0", "latest", "dev"],
    }


async def test_trigger_flow_run_uses_newest_prefect_flow_version_when_version_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the newest deployment version when no version is requested."""
    deployment_id = uuid4()
    deployments = [
        SimpleNamespace(id=uuid4(), name="grow_optimizer:0.10.1"),
        SimpleNamespace(id=uuid4(), name="grow_optimizer:latest"),
        SimpleNamespace(id=uuid4(), name="grow_optimizer:0.10.2-rc.1"),
        SimpleNamespace(id=deployment_id, name="grow_optimizer:0.10.2"),
    ]
    fake_client = _FakeClientContext(deployments)
    monkeypatch.setattr(prefect_util, "get_client", lambda: fake_client)

    run_id = await prefect_util.trigger_flow_run(
        run_name="job-123",
        deployment_base_name="grow_optimizer",
        deployment_version=None,
    )

    assert isinstance(run_id, UUID)
    assert fake_client.read_deployments_kwargs == {
        "sort": prefect_util.DeploymentSort.CREATED_DESC,
    }
    assert fake_client.create_flow_run_kwargs == {
        "deployment_id": deployment_id,
        "parameters": {},
        "name": "job-123",
        "tags": ["version:0.10.2"],
        "job_variables": None,
    }


async def test_trigger_flow_run_raises_when_deployment_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise a clear error when the requested deployment does not exist."""
    fake_client = _FakeClientContext([])
    monkeypatch.setattr(prefect_util, "get_client", lambda: fake_client)

    with pytest.raises(RuntimeError, match="Prefect deployment 'grow_optimizer:0.10.2' not found for run 'job-123'"):
        await prefect_util.trigger_flow_run(
            run_name="job-123",
            deployment_base_name="grow_optimizer",
            deployment_version="0.10.2",
        )


def test_get_workflows_returns_503_when_prefect_unreachable(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """Return HTTP 503 when Prefect cannot be reached while listing workflows."""

    async def _raise_prefect_unreachable(_: list[str]) -> dict[str, list[str]]:
        raise RuntimeError("Prefect server is unavailable at http://prefect:4200/api. Start Prefect server.")

    monkeypatch.setattr(workflow_registry, "get_flow_versions_by_name", _raise_prefect_unreachable)

    response = client.get("/workflow/")

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "Prefect server is unavailable at http://prefect:4200/api. Start Prefect server."
    )


def test_create_job_returns_404_when_prefect_deployment_unavailable(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """Return HTTP 404 with a clear message when a Prefect deployment is missing."""

    async def _fake_get_workflow_definition(_: str) -> WorkflowDefinition:
        return WorkflowDefinition(
            workflow_type_name="grow_optimizer_default",
            workflow_type_description_name="Draft Design - Optimization",
            prefect_flow_name="grow_optimizer",
        )

    async def _raise_deployment_missing(**_: object) -> UUID:
        raise RuntimeError("RuntimeError: Prefect deployment 'grow_optimizer:0.10.2' not found for run 'job-123'")

    deleted_resources: list[MinioResource] = []
    monkeypatch.setattr(workflow_registry, "get_workflow_definition", _fake_get_workflow_definition)
    monkeypatch.setattr(job_routes, "trigger_flow_run", _raise_deployment_missing)
    monkeypatch.setattr(job_routes, "_delete_input_esdl", deleted_resources.append)

    response = client.post(
        "/job/",
        json={
            "job_name": "job-123",
            "workflow_type": "grow_optimizer_default",
            "version": "0.10.2",
            "user_name": "alice",
            "input_esdl": "aW5wdXQ=",
            "input_params_dict": {},
        },
    )

    assert response.status_code == 404
    assert "Flow deployment 'grow_optimizer:0.10.2' is not available in Prefect" in response.json()["detail"]
    assert len(deleted_resources) == 1


def test_create_job_stores_large_input_esdl_and_passes_reference_to_prefect(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, job_store: InMemoryJobStore
) -> None:
    """Store large input ESDL in MinIO and pass only its URI to Prefect."""

    async def fake_get_workflow_definition(_: str) -> WorkflowDefinition:
        return WorkflowDefinition(
            workflow_type_name="grow_optimizer_default",
            workflow_type_description_name="Draft Design - Optimization",
            prefect_flow_name="grow_optimizer",
        )

    job_id = uuid4()
    trigger_arguments: dict[str, object] = {}

    async def fake_trigger_flow_run(**kwargs: object) -> UUID:
        trigger_arguments.update(kwargs)
        return job_id

    monkeypatch.setattr(workflow_registry, "get_workflow_definition", fake_get_workflow_definition)
    monkeypatch.setattr(job_routes, "trigger_flow_run", fake_trigger_flow_run)

    response = client.post(
        "/job/",
        json={
            "job_name": "large-job",
            "workflow_type": "grow_optimizer_default",
            "user_name": "alice",
            "input_esdl": base64.b64encode(b"x" * 600_000).decode("ascii"),
            "input_params_dict": {},
        },
    )

    assert response.status_code == 200
    parameters = cast(dict[str, object], trigger_arguments["parameters"])
    flow_results_folder = parameters["flow_results_folder"]
    assert isinstance(flow_results_folder, str)
    assert re.fullmatch(r"large-job-\d{8}-\d{2}h\d{2}m\d{2}s-[0-9a-f]{8}", flow_results_folder)
    assert parameters == {
        "input_esdl_minio_path": f"s3://prefect-results/flow-results/{flow_results_folder}/input.esdl",
        "flow_results_folder": flow_results_folder,
        "workflow_type_name": "grow_optimizer_default",
        "workflow_config": {},
    }
    assert len(job_store.resources[job_id]) == 1


def test_created_job_metadata_survives_prefect_history_deletion(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """Return durable identity, metadata, and status after Prefect history is removed."""
    job_id = uuid4()

    async def fake_get_workflow_definition(_: str) -> WorkflowDefinition:
        return WorkflowDefinition(
            workflow_type_name="grow_optimizer_default",
            workflow_type_description_name="Draft Design - Optimization",
            prefect_flow_name="grow_optimizer",
        )

    async def fake_trigger_flow_run(**_: object) -> UUID:
        return job_id

    async def raise_missing_flow_run(*_args: object, **_kwargs: object) -> None:
        raise ObjectNotFound(Exception("Flow run not found"))

    monkeypatch.setattr(workflow_registry, "get_workflow_definition", fake_get_workflow_definition)
    monkeypatch.setattr(job_routes, "trigger_flow_run", fake_trigger_flow_run)

    payload = {
        "job_name": "job-123",
        "workflow_type": "grow_optimizer_default",
        "version": "0.10.2",
        "user_name": "alice",
        "input_esdl": "aW5wdXQ=",
        "input_params_dict": {},
    }
    create_response = client.post("/job/", json=payload)
    monkeypatch.setattr(job_routes, "get_flow_run_status_and_results", raise_missing_flow_run)
    get_response = client.get(f"/job/{job_id}")

    assert create_response.status_code == 200
    assert create_response.json() == {"job_id": str(job_id), "status": "ENQUEUED"}
    assert get_response.status_code == 200
    assert get_response.json() == {
        "job_id": str(job_id),
        "job_name": "job-123",
        "status": "ENQUEUED",
        "user_name": "alice",
        "workflow_type": "grow_optimizer_default",
        "progress_fraction": None,
        "progress_message": None,
        "input_esdl": None,
        "output_esdl": None,
        "input_params_dict": {},
        "timeout_after_s": 3600,
        "logs": "",
        "esdl_feedback": [],
        "job_priority": None,
    }


def test_register_job_cleanup_resources_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    job_store: InMemoryJobStore,
) -> None:
    """Register the same cleanup location repeatedly without duplicating it."""
    job_id = uuid4()

    async def fake_get_workflow_definition(_: str) -> WorkflowDefinition:
        return WorkflowDefinition(
            workflow_type_name="grow_optimizer_default",
            workflow_type_description_name="Draft Design - Optimization",
            prefect_flow_name="grow_optimizer",
        )

    async def fake_trigger_flow_run(**_: object) -> UUID:
        return job_id

    monkeypatch.setattr(workflow_registry, "get_workflow_definition", fake_get_workflow_definition)
    monkeypatch.setattr(job_routes, "trigger_flow_run", fake_trigger_flow_run)
    client.post(
        "/job/",
        json={
            "job_name": "job-123",
            "workflow_type": "grow_optimizer_default",
            "user_name": "alice",
            "input_esdl": "aW5wdXQ=",
            "input_params_dict": {},
        },
    )
    resources = {
        "version": 1,
        "resources": [
            {
                "type": "minio",
                "host": "localhost",
                "port": 9000,
                "path": f"flow-results/{job_id}",
            }
        ],
    }

    first_response = client.post(f"/job/{job_id}/cleanup-resources", json=resources)
    second_response = client.post(f"/job/{job_id}/cleanup-resources", json=resources)

    assert first_response.status_code == 204
    assert second_response.status_code == 204
    assert len(job_store.resources[job_id]) == 2
    registered_paths = [
        resource.resource.path
        for resource in job_store.resources[job_id].values()
        if isinstance(resource.resource, MinioResource)
    ]
    assert registered_paths.count(f"flow-results/{job_id}") == 1
