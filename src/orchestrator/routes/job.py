import base64
import binascii
import json
import logging
from typing import cast
from uuid import UUID

from fastapi import APIRouter, HTTPException
from omotes_sdk.prefect_util import (
    delete_run,
    from_prefect_state_type_to_job_status,
    get_flow_run_status_and_results,
    get_runs,
    trigger_flow_run,
)

from orchestrator import workflow_registry
from orchestrator.models import (
    JobDeleteResponse,
    JobInput,
    JobResponse,
    JobStatus,
    JobStatusResponse,
    JobSummary,
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


@router.post("/", response_model=JobStatusResponse)
async def create_job(job_input: JobInput) -> JobStatusResponse:
    """Start new job: 'input_params_dict' can have lists and (nested) dicts as values."""
    workflow_definition = await workflow_registry.get_workflow_definition(job_input.workflow_type)
    run_tags: list[str] = []
    if job_input.workflow_type:
        run_tags.append(f"type:{job_input.workflow_type}")
    if job_input.user_name:
        run_tags.append(f"user:{job_input.user_name}")

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
    flow_runs = await get_runs()
    jobs: list[JobSummary] = []
    for run in flow_runs:
        if run.state is None:
            continue

        tags_by_key: dict[str, str] = {}
        for tag in run.tags or []:
            if ":" in tag:
                tag_key, tag_value = tag.split(":", 1)
                tags_by_key[tag_key] = tag_value

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

    run_name, state_type, input_parameters, tags, artifacts, logs = await get_flow_run_status_and_results(
        job_uuid, settings.minio_host, settings.minio_port, settings.minio_access_key, settings.minio_secret
    )
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
    """Delete job: terminate if running."""
    try:
        job_uuid = UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format") from None

    deleted = await delete_run(job_uuid)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Unknown job {job_id}")

    logger.info(
        "delete_job status=DELETED job_name=%s workflow_type=%s user_name=%s",
        "unknown",
        "unknown",
        "unknown",
    )

    #  TODO delete time series data if present (db in influxdb and schema in postgres?)

    return JobDeleteResponse(job_id=job_uuid, deleted=True)
