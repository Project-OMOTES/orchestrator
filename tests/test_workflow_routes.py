import json
from collections.abc import Iterator
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from omotes_sdk import prefect_util

import orchestrator.main as app_main
from orchestrator import workflow_registry
from orchestrator.main import create_app


@pytest.fixture
def client() -> Iterator[TestClient]:
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


def test_workflow_upload_replaces_in_memory_list(monkeypatch, client: TestClient) -> None:
    workflow_registry._workflows = []
    monkeypatch.setattr("orchestrator.routes.workflow.get_flow_versions_by_name", _fake_get_flow_versions_by_name)

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
            "workflow_parameters": [
                {
                    "parameter_type": "duration",
                    "key_name": "timestep",
                    "default": 3600,
                }
            ],
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
            "workflow_parameters": [],
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
            "workflow_parameters": [
                {
                    "parameter_type": "duration",
                    "key_name": "timestep",
                    "default": 3600,
                }
            ],
        }
    ]

    response = client.get("/workflow/")

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "simulator",
            "description": "Conceptual Design - Simulation",
            "versions": ["latest"],
            "workflow_parameters": [
                {
                    "parameter_type": "duration",
                    "key_name": "timestep",
                    "default": 3600,
                }
            ],
        }
    ]


def test_workflow_settings_file_is_loaded_at_startup(tmp_path, monkeypatch) -> None:
    workflow_registry._workflows = []
    monkeypatch.setattr("orchestrator.routes.workflow.get_flow_versions_by_name", _fake_get_flow_versions_by_name)
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
            "workflow_parameters": [],
        }
    ]


class _FakeClientContext:
    def __init__(self, deployments):
        self._deployments = deployments
        self.read_deployments_kwargs = None
        self.create_flow_run_kwargs = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def read_deployments(self, **kwargs):
        self.read_deployments_kwargs = kwargs
        return self._deployments

    async def create_flow_run_from_deployment(self, **kwargs):
        self.create_flow_run_kwargs = kwargs
        return SimpleNamespace(id=uuid4())


async def test_get_flow_versions_by_name_sorted_allows_any_versions(monkeypatch) -> None:
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


async def test_trigger_flow_run_uses_newest_prefect_version_when_version_missing(monkeypatch) -> None:
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


async def test_trigger_flow_run_raises_when_deployment_missing(monkeypatch) -> None:
    fake_client = _FakeClientContext([])
    monkeypatch.setattr(prefect_util, "get_client", lambda: fake_client)

    with pytest.raises(RuntimeError, match="Prefect deployment 'grow_optimizer:0.10.2' not found for run 'job-123'"):
        await prefect_util.trigger_flow_run(
            run_name="job-123",
            deployment_base_name="grow_optimizer",
            deployment_version="0.10.2",
        )
