import asyncio
import base64
import binascii
import json
import logging
from typing import cast
from uuid import UUID

import httpx
from fastapi import APIRouter, HTTPException
from omotes_sdk.prefect_util import (
    JOB_CLEANUP_RESOURCES_ARTIFACT_KEY,
    JobCleanupResources,
    MinioResource,
    TimeseriesResource,
    delete_run,
    from_prefect_state_type_to_job_status,
    get_flow_run_status_and_results,
    get_runs,
    trigger_flow_run,
)
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import ArtifactFilter, ArtifactFilterFlowRunId
from prefect.client.schemas.responses import SetStateStatus
from prefect.exceptions import ObjectNotFound, PrefectHTTPStatusError
from prefect.states import Cancelling
from pydantic import ValidationError

from orchestrator import workflow_registry
from orchestrator.models import (
    JobDeleteResponse,
    JobInput,
    JobResponse,
    JobStatus,
    JobStatusResponse,
    JobSummary,
)
from orchestrator.prefect_errors import raise_for_prefect_client_error, raise_for_prefect_runtime_error
from orchestrator.resource_cleanup import (
    CleanupIssueKind,
    ResourceCleanupBatchError,
    ResourceCleanupError,
    cleanup_resources,
)
from orchestrator.settings import settings

logger = logging.getLogger("orchestrator")

_TERMINAL_JOB_STATUSES = {
    JobStatus.SUCCEEDED,
    JobStatus.CANCELLED,
    JobStatus.TIMEOUT,
    JobStatus.ERROR,
}

router = APIRouter(prefix="/job", tags=["job"])


def _decode_input_esdl(input_esdl: str) -> str:
    try:
        return base64.b64decode(input_esdl, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="input_esdl must be valid base64-encoded UTF-8 text") from exc


def _b64_encode_esdl_str(output_esdl: object) -> str | None:
    if not isinstance(output_esdl, str):
        return None

    return base64.b64encode(output_esdl.encode("utf-8")).decode("ascii")


def _find_artifact_by_prefix(artifacts: dict[str, dict], prefix: str) -> dict | None:
    """Find artifact by key prefix (e.g., 'output-esdl' matches 'output-esdl-f7161df7')."""
    for key in artifacts:
        if key.startswith(prefix):
            return artifacts[key]
    return None


def _parse_artifact_data(data: object) -> object:
    """Parse artifact payload when it is serialized as JSON text."""
    if not isinstance(data, str):
        return data

    stripped = data.strip()
    if not stripped:
        return data

    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return data


def _get_tags_by_key(tags: list[str] | None) -> dict[str, str]:
    tags_by_key: dict[str, str] = {}
    for tag in tags or []:
        if ":" in tag:
            tag_key, tag_value = tag.split(":", 1)
            tags_by_key[tag_key] = tag_value
    return tags_by_key


def _get_esdl_feedback(esdl_messages: object) -> list[dict]:
    if not esdl_messages:
        return []

    raw_messages: list[object]
    if isinstance(esdl_messages, dict):
        messages = esdl_messages.get("messages", [])
        raw_messages = cast(list[object], messages) if isinstance(messages, list) else []
    elif isinstance(esdl_messages, list):
        raw_messages = cast(list[object], esdl_messages)
    else:
        return []

    esdl_feedback: list[dict] = []
    for message in raw_messages:
        if not isinstance(message, dict):
            continue

        esdl_object_id = message.get("esdl_object_id") or "general"
        technical_message = message.get("technical_message") or message.get("message") or ""
        severity_name = message.get("severity")
        id_feedback = next(
            (feedback for feedback in esdl_feedback if feedback["assetID"] == esdl_object_id),
            None,
        )
        feedback_message = {
            "validation_message": technical_message,
            "severity": severity_name,
        }

        if id_feedback:
            id_feedback["messages"].append(feedback_message)
        else:
            esdl_feedback.append({"assetID": esdl_object_id, "messages": [feedback_message]})

    return esdl_feedback


async def _prepare_flow_run_deletion(
    flow_run_id: UUID,
) -> tuple[str, str, str, list[MinioResource | TimeseriesResource]]:
    """Cancel a flow run and collect resources to remove before deleting its history.

    Returns:
        A tuple containing the flow-run name, workflow type, user name, and cleanup resources.
        If the flow run is not found, the metadata fields are ``"unknown"`` and the resource list is empty.
    """
    try:
        async with get_client() as client:
            flow_run = await client.read_flow_run(flow_run_id)
            if flow_run.state is None:
                raise HTTPException(status_code=409, detail=f"Job {flow_run_id} has no Prefect state")

            if not flow_run.state.is_final():
                cancellation_result = await client.set_flow_run_state(flow_run_id, Cancelling())
                if cancellation_result.status != SetStateStatus.ACCEPT:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Prefect did not accept cancellation for job {flow_run_id}",
                    )

                deadline = asyncio.get_running_loop().time() + settings.cancellation_timeout_seconds
                while True:
                    flow_run = await client.read_flow_run(flow_run_id)
                    if flow_run.state is not None and flow_run.state.is_cancelled():
                        break
                    if flow_run.state is not None and flow_run.state.is_final():
                        raise HTTPException(
                            status_code=409,
                            detail=f"Job {flow_run_id} reached {flow_run.state.type.name} instead of CANCELLED",
                        )

                    remaining_seconds = deadline - asyncio.get_running_loop().time()
                    if remaining_seconds <= 0:
                        raise HTTPException(
                            status_code=504,
                            detail=f"Timed out waiting for job {flow_run_id} to cancel",
                        )
                    await asyncio.sleep(min(settings.cancellation_poll_interval_seconds, remaining_seconds))
            elif not (flow_run.state.is_cancelled() or flow_run.state.is_completed()):
                raise HTTPException(
                    status_code=409,
                    detail=f"Job {flow_run_id} is already {flow_run.state.type.name} and cannot be cancelled",
                )

            artifacts = await client.read_artifacts(
                artifact_filter=ArtifactFilter(flow_run_id=ArtifactFilterFlowRunId(any_=[flow_run_id]))
            )
    except ObjectNotFound:
        return "unknown", "unknown", "unknown", []
    except (PrefectHTTPStatusError, httpx.RequestError) as exc:
        raise_for_prefect_client_error(exc)
        raise

    tags_by_key = _get_tags_by_key(flow_run.tags)

    cleanup_resource_locations: list[MinioResource | TimeseriesResource] = []
    for artifact in artifacts:
        if artifact.key == JOB_CLEANUP_RESOURCES_ARTIFACT_KEY:
            cleanup_resource_locations.extend(_parse_cleanup_resources_artifact(artifact.data))

    return flow_run.name, tags_by_key.get("type", ""), tags_by_key.get("user", ""), cleanup_resource_locations


def _parse_cleanup_resources_artifact(data: object) -> list[MinioResource | TimeseriesResource]:
    """Parse the single row in a cleanup-resources table artifact."""
    parsed_data = _parse_artifact_data(data)
    if isinstance(parsed_data, list) and len(parsed_data) == 1:
        parsed_data = parsed_data[0]

    try:
        return JobCleanupResources.model_validate(parsed_data).resources
    except ValidationError as exc:
        raise ResourceCleanupError("Invalid job cleanup resources artifact") from exc


@router.post("/", response_model=JobStatusResponse)
async def create_job(job_input: JobInput) -> JobStatusResponse:
    """Start new job: 'input_params_dict' can have lists and (nested) dicts as values."""
    workflow_definition = await workflow_registry.get_workflow_definition(job_input.workflow_type)
    run_tags: list[str] = []
    if job_input.workflow_type:
        run_tags.append(f"type:{job_input.workflow_type}")
    if job_input.user_name:
        run_tags.append(f"user:{job_input.user_name}")

    try:
        run_id = await trigger_flow_run(
            run_name=job_input.job_name,
            deployment_base_name=workflow_definition.prefect_flow_name,
            deployment_version=job_input.version,
            parameters={
                "input_esdl": _decode_input_esdl(job_input.input_esdl),
                "workflow_type_name": job_input.workflow_type,
                "workflow_config": job_input.input_params_dict,
            },
            run_tags=run_tags,
            memory_limit=workflow_definition.memory_limit,
        )
    except RuntimeError as exc:
        raise_for_prefect_runtime_error(exc)
        raise

    logger.info(
        "create_job job_name=%s workflow_type=%s workflow_version=%s user_name=%s",
        job_input.job_name,
        job_input.workflow_type,
        job_input.version,
        job_input.user_name,
    )

    return JobStatusResponse(
        job_id=run_id,
        status=JobStatus.ENQUEUED,
    )


@router.get("/", response_model=list[JobSummary])
async def list_jobs() -> list[JobSummary]:
    """Return a summary of all jobs."""
    try:
        flow_runs = await get_runs()
    except RuntimeError as exc:
        raise_for_prefect_runtime_error(exc)
        raise
    jobs: list[JobSummary] = []
    for run in flow_runs:
        if run.state is None:
            continue

        tags_by_key = _get_tags_by_key(run.tags)

        jobs.append(
            JobSummary(
                job_id=run.id,
                job_name=run.name,
                status=from_prefect_state_type_to_job_status(run.state.type),
                user_name=tags_by_key.get("user", ""),
                project_name="",
            )
        )

    return jobs


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str) -> JobResponse:
    """Return job details."""
    try:
        job_uuid = UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format") from None

    try:
        run_name, state_type, input_parameters, tags, artifacts, logs = await get_flow_run_status_and_results(
            job_uuid, settings.minio_host, settings.minio_port, settings.minio_access_key, settings.minio_secret
        )
    except ObjectNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Unknown job {job_id}") from exc
    except (PrefectHTTPStatusError, httpx.RequestError) as exc:
        raise_for_prefect_client_error(exc)
        raise
    except RuntimeError as exc:
        raise_for_prefect_runtime_error(exc)
        raise
    status = from_prefect_state_type_to_job_status(state_type)

    if status in _TERMINAL_JOB_STATUSES:
        logger.info(
            "get_job status=%s job_name=%s workflow_type=%s user_name=%s",
            status,
            run_name,
            tags.get("type", ""),
            tags.get("user", ""),
        )

    output_esdl_artifact = _find_artifact_by_prefix(artifacts, "output-esdl")
    output_esdl_data = output_esdl_artifact.get("data") if output_esdl_artifact else None
    esdl_messages_artifact = _find_artifact_by_prefix(artifacts, "esdl-messages")
    esdl_messages_data = _parse_artifact_data(esdl_messages_artifact.get("data")) if esdl_messages_artifact else None
    progress_artifact = _find_artifact_by_prefix(artifacts, "progress")

    return JobResponse(
        job_id=job_uuid,
        job_name=run_name,
        status=status,
        user_name=tags.get("user", ""),
        workflow_type=tags.get("type", ""),
        input_esdl=_b64_encode_esdl_str(input_parameters.get("input_esdl")),
        output_esdl=_b64_encode_esdl_str(output_esdl_data),
        input_params_dict=input_parameters.get("workflow_config", {}),
        timeout_after_s=input_parameters.get("timeout_after_s", 3600),
        job_priority=input_parameters.get("job_priority"),
        esdl_feedback=_get_esdl_feedback(esdl_messages_data),
        progress_fraction=progress_artifact.get("data") if progress_artifact else None,
        progress_message=progress_artifact.get("description") if progress_artifact else None,
        logs=logs,
    )


@router.delete("/{job_id}", response_model=JobDeleteResponse)
async def delete_job(job_id: str) -> JobDeleteResponse:
    """Cancel a job, wait for it to stop, then remove its Prefect run and cleanup associated resources."""
    try:
        job_uuid = UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format") from None

    job_name, workflow_type, user_name, resource_locations = await _prepare_flow_run_deletion(job_uuid)

    cleanup_error: ResourceCleanupBatchError | None = None
    try:
        cleanup_resources(resource_locations, settings)
    except ResourceCleanupBatchError as exc:
        cleanup_error = exc

    try:
        deleted = await delete_run(job_uuid)
    except RuntimeError as exc:
        raise_for_prefect_runtime_error(exc)
        raise
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Unknown prefect job '{job_id}'.")

    if cleanup_error is not None:
        logger.error(
            "delete_job status=CLEANUP_FAILED job_name=%s workflow_type=%s user_name=%s "
            "history=DELETED connection_failed=%s delete_failed=%s data_not_found=%s",
            job_name,
            workflow_type,
            user_name,
            [str(issue) for issue in cleanup_error.failures if issue.kind == CleanupIssueKind.CONNECTION_FAILED],
            [str(issue) for issue in cleanup_error.failures if issue.kind == CleanupIssueKind.DELETE_FAILED],
            [str(issue) for issue in cleanup_error.not_found],
        )

    logger.info(
        "delete_job status=DELETED job_name=%s workflow_type=%s user_name=%s",
        job_name,
        workflow_type,
        user_name,
    )

    return JobDeleteResponse(job_id=job_uuid, deleted=True)
