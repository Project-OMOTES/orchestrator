import json
from collections.abc import Iterator, Sequence
from pathlib import Path
from types import SimpleNamespace, TracebackType
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from omotes_sdk import prefect_util

import orchestrator.main as app_main
from orchestrator import workflow_registry
from orchestrator.main import create_app


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
