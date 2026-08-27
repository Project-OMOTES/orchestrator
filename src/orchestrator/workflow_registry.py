"""In-memory registry for available workflows."""

import asyncio
from pathlib import Path

from fastapi import HTTPException
from omotes_sdk import MemoryLimit
from omotes_sdk.prefect_util import get_flow_versions_by_name
from pydantic import TypeAdapter

from orchestrator.models import WorkflowResponse, WorkflowUpload
from orchestrator.workflow_types import WorkflowDefinition

_workflows: list[WorkflowDefinition] = []
_workflows_lock = asyncio.Lock()
_workflow_upload_adapter = TypeAdapter(list[WorkflowUpload])


def _to_workflow_type(workflow: WorkflowUpload) -> WorkflowDefinition:
    return WorkflowDefinition(
        workflow_type_name=workflow.workflow_type_name,
        workflow_type_description_name=workflow.workflow_type_description_name,
        prefect_flow_name=workflow.prefect_flow_name,
        workflow_parameters=dict(workflow.workflow_parameters),
        memory_limit=MemoryLimit(workflow.memory_limit) if workflow.memory_limit else None,
    )


async def get_snapshot() -> list[WorkflowDefinition]:
    """Return a snapshot of the current workflows list."""
    async with _workflows_lock:
        return list(_workflows)


async def replace(workflows: list[WorkflowDefinition]) -> None:
    """Replace the workflows list with a new one."""
    global _workflows
    async with _workflows_lock:
        _workflows = list(workflows)


async def get_workflow_definition(workflow_type_name: str) -> WorkflowDefinition:
    """Get the workflow definition for a given workflow type.

    Raises:
        HTTPException: If the workflow type is not found.
    """
    workflows = await get_snapshot()
    for workflow_type in workflows:
        if workflow_type.workflow_type_name == workflow_type_name:
            return workflow_type

    raise HTTPException(status_code=404, detail=f"Unknown workflow type {workflow_type_name}")


async def load_from_file(file_path: str) -> list[WorkflowDefinition]:
    """Load workflows from a JSON file and replace the current in-memory list.

    Returns:
        list[WorkflowDefinition]: The loaded workflows.
    """
    payload = _workflow_upload_adapter.validate_json(Path(file_path).read_text(encoding="utf-8"))
    workflows = [_to_workflow_type(workflow) for workflow in payload]
    await replace(workflows)
    return workflows


async def get_workflows_jsonforms_format_with_versions() -> list[WorkflowResponse]:
    """Get the available workflows with jsonforms schema for the non-ESDL parameters.

    :return: dictionary response.
    """
    async with _workflows_lock:
        prefect_flow_names = sorted({workflow.prefect_flow_name for workflow in _workflows})
        versions_by_flow_name = await get_flow_versions_by_name(prefect_flow_names)
        missing_flow_names = [flow_name for flow_name in prefect_flow_names if not versions_by_flow_name.get(flow_name)]
        if missing_flow_names:
            raise HTTPException(
                status_code=404,
                detail=f"Prefect deployments not found for flow name(s): {', '.join(missing_flow_names)}",
            )

        workflows_jsonforms: list[WorkflowResponse] = []
        for workflow_def in _workflows:
            workflow_reponse = WorkflowResponse(
                id=workflow_def.workflow_type_name,
                description=workflow_def.workflow_type_description_name,
                versions=versions_by_flow_name.get(workflow_def.prefect_flow_name, []),
            )

            if workflow_def.workflow_parameters:
                workflow_reponse.schema = dict(
                    type="object",
                    properties=workflow_def.workflow_parameters,
                    required=list(workflow_def.workflow_parameters.keys()),
                )

                elements: list[dict[str, str]] = []
                for param_key in workflow_def.workflow_parameters:
                    elements.append(
                        {
                            "type": "Control",
                            "scope": f"#/properties/{param_key}",
                        }
                    )
                workflow_reponse.uischema = {"type": "VerticalLayout", "elements": elements}

            workflows_jsonforms.append(workflow_reponse)
        return workflows_jsonforms
