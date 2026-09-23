import json
import logging
from collections.abc import Iterator, Sequence
from pathlib import Path
from types import SimpleNamespace, TracebackType
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from omotes_sdk import prefect_util
from omotes_sdk.prefect_util import JOB_CLEANUP_RESOURCES_ARTIFACT_KEY, MinioResource, TimeseriesResource
from prefect.exceptions import ObjectNotFound
from prefect.states import Cancelled, Completed, Running, StateType

import orchestrator.main as app_main
from orchestrator import resource_cleanup, workflow_registry
from orchestrator.main import create_app
from orchestrator.routes import job as job_routes
from orchestrator.settings import settings
from orchestrator.workflow_types import WorkflowDefinition


@pytest.fixture
def client() -> Iterator[TestClient]:
    """Create a test client for the application."""
    # Keep TestClient lifecycle explicit so AnyIO portal teardown is deterministic in debug sessions.
    with TestClient(create_app()) as test_client:
        yield test_client


async def _fake_get_flow_versions_by_name(flow_names: list[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    if "grow_optimizer" in flow_names:
        result["grow_optimizer"] = ["0.10.1", "0.10.2"]
    if "simulator" in flow_names:
        result["simulator"] = ["latest"]
    return result


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
    """Return 404 instead of leaking Prefect's missing flow run exception."""
    job_id = uuid4()

    async def raise_missing_flow_run(*_args: object, **_kwargs: object) -> None:
        raise ObjectNotFound(Exception("Flow run not found"))

    monkeypatch.setattr(job_routes, "get_flow_run_status_and_results", raise_missing_flow_run)

    response = client.get(f"/job/{job_id}")

    assert response.status_code == 404
    assert response.json() == {"detail": f"Unknown job {job_id}"}


def test_delete_job_logs_flow_run_metadata(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """Log the deleted job's name and identifying tags."""
    job_id = uuid4()

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
                tags=["type:grow_optimizer_default", "user:tolga"],
                state=Cancelled(),
            )

        async def read_artifacts(self, **_kwargs: object) -> list[SimpleNamespace]:
            return []

    async def delete_existing_flow_run(flow_run_id: UUID) -> bool:
        assert flow_run_id == job_id
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    with caplog.at_level(logging.INFO, logger="orchestrator"):
        response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert (
        "delete_job status=DELETED job_name=asset-constraints workflow_type=grow_optimizer_default user_name=tolga"
        in caplog.messages
    )


def test_delete_job_deletes_completed_flow(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """Delete a successfully completed flow run without requesting cancellation."""
    job_id = uuid4()
    delete_called = False

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, flow_run_id: UUID) -> SimpleNamespace:
            assert flow_run_id == job_id
            return SimpleNamespace(name="completed-job", tags=[], state=Completed())

        async def read_artifacts(self, **_kwargs: object) -> list[SimpleNamespace]:
            return []

    async def delete_existing_flow_run(flow_run_id: UUID) -> bool:
        nonlocal delete_called
        assert flow_run_id == job_id
        delete_called = True
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert delete_called


def test_delete_job_requests_cancellation_for_running_flow(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """Only delete an active flow run after Prefect confirms cancellation."""
    job_id = uuid4()

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

        async def read_artifacts(self, **_kwargs: object) -> list[SimpleNamespace]:
            return []

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

    assert response.status_code == 200
    assert fake_client.cancelled_flow_run_id == job_id


def test_delete_job_cleans_declared_resources_before_deleting_history(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """Use the worker's cleanup artifact before deleting the Prefect flow run."""
    job_id = uuid4()
    cleaned_resources: list[MinioResource] = []
    delete_called = False

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            return SimpleNamespace(name="finished-job", tags=[], state=Cancelled())

        async def read_artifacts(self, **_kwargs: object) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(
                    key=JOB_CLEANUP_RESOURCES_ARTIFACT_KEY,
                    data=[
                        {
                            "version": 1,
                            "resources": [
                                {
                                    "type": "minio",
                                    "host": "omotes-minio",
                                    "port": 9000,
                                    "path": "flow-results/run-id",
                                }
                            ],
                        }
                    ],
                )
            ]

    def clean_resources(resources: list[MinioResource], _settings: object) -> None:
        cleaned_resources.extend(resources)

    async def delete_existing_flow_run(_: UUID) -> bool:
        nonlocal delete_called
        assert cleaned_resources
        delete_called = True
        return True

    monkeypatch.setattr(job_routes, "get_client", lambda: FakeClientContext())
    monkeypatch.setattr(job_routes, "cleanup_resources", clean_resources)
    monkeypatch.setattr(job_routes, "delete_run", delete_existing_flow_run)

    response = client.delete(f"/job/{job_id}")

    assert response.status_code == 200
    assert delete_called
    assert cleaned_resources == [MinioResource(host="omotes-minio", port=9000, path="flow-results/run-id")]


def test_delete_job_reports_cleanup_failure_after_deleting_run(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """Delete the Prefect record even when a declared resource cannot be cleaned up."""
    job_id = uuid4()
    delete_called = False

    class FakeClientContext:
        async def __aenter__(self) -> "FakeClientContext":
            return self

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        async def read_flow_run(self, _: UUID) -> SimpleNamespace:
            return SimpleNamespace(name="finished-job", tags=[], state=Cancelled())

        async def read_artifacts(self, **_kwargs: object) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(
                    key=JOB_CLEANUP_RESOURCES_ARTIFACT_KEY,
                    data=[
                        {
                            "version": 1,
                            "resources": [
                                {
                                    "type": "minio",
                                    "host": "unconfigured-minio",
                                    "port": 9000,
                                    "path": "flow-results/run-id",
                                }
                            ],
                        }
                    ],
                )
            ]

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
    assert "No MinIO credentials configured for cleanup resource host=unconfigured-minio port=9000" in caplog.messages
    assert any(
        "delete_job status=CLEANUP_FAILED job_name=finished-job workflow_type= user_name= "
        "history=DELETED connection_failed=" in message
        for message in caplog.messages
    )
    assert response.json() == {"job_id": str(job_id), "deleted": True}


def test_minio_cleanup_limits_deletion_to_one_run_folder(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use a trailing slash so MinIO prefix matching cannot include sibling runs."""
    prefixes: list[str] = []

    class FakeMinio:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def list_objects(self, _bucket: str, prefix: str, recursive: bool) -> list[object]:
            assert recursive
            prefixes.append(prefix)
            return [SimpleNamespace(object_name="flow-results/run-1/result.json")]

        def remove_object(self, _bucket: str, _object_name: str) -> None:
            return None

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

    with pytest.raises(resource_cleanup.ResourceCleanupError, match="non-UUID"):
        resource_cleanup._delete_influxdb_resource(resource, settings)


def test_influx_cleanup_logs_missing_database(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Treat an already-removed Influx database as an idempotent cleanup result."""

    class FakeInfluxClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def get_list_database(self) -> list[dict[str, str]]:
            return []

    monkeypatch.setattr(resource_cleanup, "InfluxDBClient", FakeInfluxClient)
    resource = TimeseriesResource(
        type="influxdb",
        host=settings.influx_host or "",
        port=settings.influx_port or 0,
        database=str(uuid4()),
    )

    with (
        caplog.at_level(logging.WARNING, logger="orchestrator"),
        pytest.raises(resource_cleanup.ResourceCleanupBatchError) as error,
    ):
        resource_cleanup.cleanup_resources([resource], settings)

    assert any("Cleanup resource data not found type=influxdb" in message for message in caplog.messages)
    assert error.value.failures == []
    assert error.value.not_found[0].kind == resource_cleanup.CleanupIssueKind.DATA_NOT_FOUND


def test_cleanup_resources_classifies_database_connection_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Classify database connection failures separately from cleanup data errors."""
    resource = TimeseriesResource(
        type="postgresql",
        host=settings.postgres_host or "",
        port=settings.postgres_port or 0,
        database="any_database",
        schema_name=str(uuid4()),
    )

    def raise_connection_failure(*_args: object, **_kwargs: object) -> None:
        raise resource_cleanup.psycopg.OperationalError("database is unreachable")

    monkeypatch.setattr(resource_cleanup, "_delete_postgresql_resource", raise_connection_failure)

    with pytest.raises(resource_cleanup.ResourceCleanupBatchError) as error:
        resource_cleanup.cleanup_resources([resource], settings)

    assert error.value.failures[0].detail == "database is unreachable"


def test_cleanup_resources_continues_after_resource_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Attempt later resources even when an earlier resource cannot be deleted."""
    resources = [
        TimeseriesResource(
            type="postgresql",
            host=settings.postgres_host or "",
            port=settings.postgres_port or 0,
            database="first",
            schema_name=str(uuid4()),
        ),
        TimeseriesResource(
            type="postgresql",
            host=settings.postgres_host or "",
            port=settings.postgres_port or 0,
            database="second",
            schema_name=str(uuid4()),
        ),
    ]
    attempted: list[str] = []

    def delete_resource(resource: TimeseriesResource, _settings: object) -> None:
        attempted.append(resource.database)
        if resource.database == "first":
            raise resource_cleanup.ResourceCleanupError("delete failed")

    monkeypatch.setattr(resource_cleanup, "_delete_postgresql_resource", delete_resource)

    with pytest.raises(resource_cleanup.ResourceCleanupBatchError) as error:
        resource_cleanup.cleanup_resources(resources, settings)

    assert attempted == ["first", "second"]
    assert [issue.detail for issue in error.value.failures] == ["delete failed"]


def test_postgresql_cleanup_allows_uuid_schema_in_declared_database(monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete UUID-named schemas without imposing a database allowlist."""
    connection_kwargs: dict[str, object] = {}

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        def execute(self, _query: object, _params: object = None) -> None:
            return None

        def fetchone(self) -> tuple[int]:
            return (1,)

    class FakeConnection:
        def __enter__(self) -> "FakeConnection":
            return self

        def __exit__(
            self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
        ) -> None:
            return None

        def cursor(self) -> FakeCursor:
            return FakeCursor()

    def connect(**kwargs: object) -> FakeConnection:
        connection_kwargs.update(kwargs)
        return FakeConnection()

    monkeypatch.setattr(resource_cleanup.psycopg, "connect", connect)
    resource = TimeseriesResource(
        type="postgresql",
        host=settings.postgres_host or "",
        port=settings.postgres_port or 0,
        database="another_timeseries_database",
        schema_name=str(uuid4()),
    )

    resource_cleanup._delete_postgresql_resource(resource, settings)

    assert connection_kwargs["dbname"] == "another_timeseries_database"


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

    with pytest.raises(resource_cleanup.ResourceCleanupError, match="non-UUID"):
        resource_cleanup._delete_postgresql_resource(resource, settings)


def test_delete_job_keeps_history_when_prefect_rejects_cancellation(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """Do not delete an active run when Prefect rejects its cancellation request."""
    job_id = uuid4()
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

    assert response.status_code == 409
    assert not delete_called


def test_delete_job_returns_503_when_prefect_metadata_read_is_unavailable(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """Map metadata lookup connectivity failures to the existing upstream error contract."""
    job_id = uuid4()

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

    response = client.delete(f"/job/{job_id}")

    assert response.status_code == 503
    assert response.json() == {"detail": "Prefect server is unavailable"}


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

    monkeypatch.setattr(workflow_registry, "get_workflow_definition", _fake_get_workflow_definition)
    monkeypatch.setattr(job_routes, "trigger_flow_run", _raise_deployment_missing)

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
