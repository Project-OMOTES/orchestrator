"""Workflow endpoints."""

import logging

import httpx
from fastapi import APIRouter, HTTPException
from omotes_sdk.prefect_util import get_flow_versions_by_name
from prefect.exceptions import PrefectHTTPStatusError

from orchestrator import workflow_registry
from orchestrator.models import WorkflowResponse, WorkflowUpload
from orchestrator.settings import settings
from orchestrator.workflow_types import WorkflowType

logger = logging.getLogger("orchestrator")

router = APIRouter(prefix="/workflow", tags=["workflow"])


def _to_workflow_type(workflow: WorkflowUpload) -> WorkflowType:
    return WorkflowType(
        workflow_type_name=workflow.workflow_type_name,
        workflow_type_description_name=workflow.workflow_type_description_name,
        prefect_flow_name=workflow.prefect_flow_name,
        workflow_parameters=list(workflow.workflow_parameters),
    )


def _to_workflow_response(workflow: WorkflowType, versions: list[str]) -> WorkflowResponse:
    return WorkflowResponse(
        id=workflow.workflow_type_name,
        description=workflow.workflow_type_description_name,
        versions=versions,
        workflow_parameters=list(workflow.workflow_parameters),
    )


async def _to_workflow_responses(workflows: list[WorkflowType]) -> list[WorkflowResponse]:
    prefect_flow_names = sorted({workflow.prefect_flow_name for workflow in workflows})
    try:
        versions_by_flow_name = await get_flow_versions_by_name(prefect_flow_names)
    except PrefectHTTPStatusError as exc:
        if exc.response.status_code == 401:
            raise HTTPException(
                status_code=401,
                detail=(
                    f"Unauthorized: Invalid or missing authentication for Prefect server at {settings.prefect_api_url}. "
                    "Check PREFECT_API_AUTH_STRING setting."
                ),
            ) from exc
        raise HTTPException(
            status_code=502,
            detail=f"Prefect server error (HTTP {exc.response.status_code}): {exc.response.reason_phrase}",
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Prefect server is unavailable at {settings.prefect_api_url}. "
                "Start Prefect server or update PREFECT_API_URL, then try again."
            ),
        ) from exc
    missing_flow_names = [flow_name for flow_name in prefect_flow_names if not versions_by_flow_name.get(flow_name)]
    if missing_flow_names:
        raise HTTPException(
            status_code=404,
            detail=f"Prefect deployments not found for flow names: {', '.join(missing_flow_names)}",
        )
    return [
        _to_workflow_response(workflow, versions_by_flow_name.get(workflow.prefect_flow_name, []))
        for workflow in workflows
    ]


async def _get_workflows() -> list[WorkflowType]:
    """Get current workflows from registry."""
    return await workflow_registry.get_snapshot()


@router.get("/", response_model=list[WorkflowResponse])
async def get_workflows() -> list[WorkflowResponse]:
    """Return the current in-memory workflows list."""
    workflows = await _get_workflows()
    return await _to_workflow_responses(workflows)


@router.post("/", response_model=list[WorkflowResponse])
async def upload_workflows(workflows_payload: list[WorkflowUpload]) -> list[WorkflowResponse]:
    """Replace the current in-memory workflows list with the posted JSON content."""
    workflows = [
        WorkflowType(
            workflow_type_name=workflow.workflow_type_name,
            workflow_type_description_name=workflow.workflow_type_description_name,
            prefect_flow_name=workflow.prefect_flow_name,
            workflow_parameters=list(workflow.workflow_parameters),
        )
        for workflow in workflows_payload
    ]
    await workflow_registry.replace(workflows)
    return await _to_workflow_responses(workflows)
