"""Workflow endpoints."""

import logging

from fastapi import APIRouter
from omotes_sdk import MemoryLimit

from orchestrator import workflow_registry
from orchestrator.models import WorkflowResponse, WorkflowUpload
from orchestrator.workflow_types import WorkflowDefinition

logger = logging.getLogger("orchestrator")

router = APIRouter(prefix="/workflow", tags=["workflow"])


def _to_workflow_type(workflow: WorkflowUpload) -> WorkflowDefinition:
    return WorkflowDefinition(
        workflow_type_name=workflow.workflow_type_name,
        workflow_type_description_name=workflow.workflow_type_description_name,
        prefect_flow_name=workflow.prefect_flow_name,
        workflow_parameters=dict(workflow.workflow_parameters),
        memory_limit=MemoryLimit(workflow.memory_limit) if workflow.memory_limit else None,
    )


@router.get("/", response_model=list[WorkflowResponse], response_model_exclude_none=True)
async def get_workflows() -> list[WorkflowResponse]:
    """Return the current in-memory workflows list."""
    return await workflow_registry.get_workflows_jsonforms_format_with_versions()


@router.post("/", response_model=list[WorkflowResponse], response_model_exclude_none=True)
async def upload_workflows(workflows_payload: list[WorkflowUpload]) -> list[WorkflowResponse]:
    """Replace the current in-memory workflows list with the posted JSON content."""
    workflows = [
        WorkflowDefinition(
            workflow_type_name=workflow.workflow_type_name,
            workflow_type_description_name=workflow.workflow_type_description_name,
            prefect_flow_name=workflow.prefect_flow_name,
            workflow_parameters=dict(workflow.workflow_parameters),
            memory_limit=MemoryLimit(workflow.memory_limit) if workflow.memory_limit else None,
        )
        for workflow in workflows_payload
    ]
    await workflow_registry.replace(workflows)
    return await workflow_registry.get_workflows_jsonforms_format_with_versions()
