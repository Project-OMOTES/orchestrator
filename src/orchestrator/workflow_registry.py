"""In-memory registry for available workflows."""

import asyncio
from pathlib import Path

from fastapi import HTTPException
from pydantic import TypeAdapter

from orchestrator.models import WorkflowUpload
from orchestrator.workflow_types import WorkflowType

_workflows: list[WorkflowType] = []
_workflows_lock = asyncio.Lock()
_workflow_upload_adapter = TypeAdapter(list[WorkflowUpload])


def _to_workflow_type(workflow: WorkflowUpload) -> WorkflowType:
    return WorkflowType(
        workflow_type_name=workflow.workflow_type_name,
        workflow_type_description_name=workflow.workflow_type_description_name,
        prefect_flow_name=workflow.prefect_flow_name,
        workflow_parameters=list(workflow.workflow_parameters),
    )


async def get_snapshot() -> list[WorkflowType]:
    """Return a snapshot of the current workflows list."""
    async with _workflows_lock:
        return list(_workflows)


async def replace(workflows: list[WorkflowType]) -> None:
    """Replace the workflows list with a new one."""
    global _workflows
    async with _workflows_lock:
        _workflows = list(workflows)


async def get_flow_name(workflow_type_name: str) -> str:
    """Get the Prefect flow name for a given workflow type.

    Raises:
        HTTPException: If the workflow type is not found.
    """
    workflows = await get_snapshot()
    for workflow_type in workflows:
        if workflow_type.workflow_type_name == workflow_type_name:
            return workflow_type.prefect_flow_name

    raise HTTPException(status_code=404, detail=f"Unknown workflow type {workflow_type_name}")


async def load_from_file(file_path: str) -> list[WorkflowType]:
    """Load workflows from a JSON file and replace the current in-memory list.

    Returns:
        list[WorkflowType]: The loaded workflows.
    """
    payload = _workflow_upload_adapter.validate_json(Path(file_path).read_text(encoding="utf-8"))
    workflows = [_to_workflow_type(workflow) for workflow in payload]
    await replace(workflows)
    return workflows
