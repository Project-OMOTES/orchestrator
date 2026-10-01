import asyncio
import base64
import binascii
import json
import logging
import re
from datetime import UTC, datetime
from enum import StrEnum
from io import BytesIO
from typing import Annotated, cast
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response, status
from minio import Minio
from omotes_sdk.prefect_util import (
    PREFECT_RESULTS_BUCKET,
    JobCleanupResources,
    MinioResource,
    delete_run,
    from_prefect_state_type_to_job_status,
    get_flow_run_status_and_results,
    get_runs,
    trigger_flow_run,
)
from prefect.client.orchestration import get_client
from prefect.client.schemas.responses import SetStateStatus
from prefect.exceptions import ObjectNotFound
from prefect.states import Cancelling, StateType

from orchestrator import workflow_registry
from orchestrator.database import Job, JobCleanupResource, JobStore, job_store
from orchestrator.models import (
    JobDeleteResponse,
    JobInput,
    JobResponse,
    JobStatus,
    JobStatusResponse,
    JobSummary,
)
from orchestrator.prefect_errors import raise_for_prefect_runtime_error
from orchestrator.resource_cleanup import cleanup_resources
from orchestrator.settings import settings

logger = logging.getLogger("orchestrator")

_TERMINAL_JOB_STATUSES = {
    JobStatus.SUCCEEDED,
    JobStatus.CANCELLED,
    JobStatus.TIMEOUT,
    JobStatus.ERROR,
}
_INPUT_ESDL_BUCKET = PREFECT_RESULTS_BUCKET

router = APIRouter(prefix="/job", tags=["job"])


def get_job_store() -> JobStore:
    """Return the durable job store for request dependency injection."""
    return job_store


JobStoreDependency = Annotated[JobStore, Depends(get_job_store)]


class CancellationWaitResult(StrEnum):
    """Outcome of waiting for a Prefect flow run to stop."""

    CANCELLED = "CANCELLED"
    MISSING = "MISSING"
    FAILED = "FAILED"


def _decode_input_esdl(input_esdl: str) -> str:
    try:
        return base64.b64decode(input_esdl, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="input_esdl must be valid base64-encoded UTF-8 text") from exc


def _minio_client() -> Minio:
    return Minio(
        f"{settings.minio_host}:{settings.minio_port}",
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret,
        secure=False,
    )


def _create_flow_results_folder(run_name: str) -> str:
    sanitized_name = re.sub(r"[^a-z0-9-]+", "-", run_name.lower().strip())
    sanitized_name = re.sub(r"-+", "-", sanitized_name).strip("-") or "job"
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%Hh%Mm%Ss")
    return f"{sanitized_name}-{timestamp}-{uuid4().hex[:8]}"


def _store_input_esdl(input_esdl: str, flow_results_folder: str) -> tuple[str, MinioResource]:
    object_path = f"flow-results/{flow_results_folder}/input.esdl"
    input_bytes = input_esdl.encode("utf-8")
    client = _minio_client()
    client.put_object(
        _INPUT_ESDL_BUCKET,
        object_path,
        BytesIO(input_bytes),
        length=len(input_bytes),
        content_type="application/xml",
    )
    resource = MinioResource(
        host=settings.minio_host,
        port=int(settings.minio_port),
        bucket=_INPUT_ESDL_BUCKET,
        path=f"flow-results/{flow_results_folder}",
    )
    return f"s3://{_INPUT_ESDL_BUCKET}/{object_path}", resource


def _delete_input_esdl(resource: MinioResource) -> None:
    _minio_client().remove_object(resource.bucket, f"{resource.path}/input.esdl")


def _cleanup_unregistered_input_esdl(resource: MinioResource, reason: str) -> None:
    try:
        _delete_input_esdl(resource)
    except Exception:
        logger.exception("create_job failed to remove stored input ESDL after %s", reason)


async def _persist_created_job(
    run_id: UUID,
    job_input: JobInput,
    input_esdl_resource: MinioResource,
    store: JobStore,
) -> None:
    try:
        await store.create_job(
            job_id=run_id,
            job_name=job_input.job_name,
            workflow_type=job_input.workflow_type,
            workflow_version=job_input.version,
            user_name=job_input.user_name,
            status=JobStatus.ENQUEUED,
        )
        await store.register_cleanup_resources(run_id, [input_esdl_resource])
    except Exception:
        logger.exception("create_job failed to persist job_id=%s; removing Prefect flow run", run_id)
        await delete_run(run_id)
        _cleanup_unregistered_input_esdl(input_esdl_resource, "job persistence failure")
        raise


def _read_input_esdl(input_reference: object) -> str | None:
    if not isinstance(input_reference, str):
        return None
    parsed = urlsplit(input_reference)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        return input_reference

    response = _minio_client().get_object(parsed.netloc, parsed.path.lstrip("/"))
    try:
        return response.read().decode("utf-8")
    finally:
        response.close()
        response.release_conn()


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


async def _wait_for_flow_run_cancellation(flow_run_id: UUID) -> CancellationWaitResult:
    """Wait until Prefect reports the flow run as cancelled."""
    try:
        async with asyncio.timeout(settings.cancellation_timeout_seconds):
            async with get_client() as client:
                while True:
                    flow_run = await client.read_flow_run(flow_run_id)
                    if flow_run.state is not None and flow_run.state.is_cancelled():
                        return CancellationWaitResult.CANCELLED
                    if flow_run.state is not None and flow_run.state.is_final():
                        logger.error(
                            "delete_job cancellation failed job_id=%s: reached %s instead of CANCELLED",
                            flow_run_id,
                            flow_run.state.type.name,
                        )
                        return CancellationWaitResult.FAILED

                    await asyncio.sleep(settings.cancellation_poll_interval_seconds)
    except ObjectNotFound:
        logger.warning("delete_job job_id=%s: Prefect history was deleted while waiting for cancellation", flow_run_id)
        return CancellationWaitResult.MISSING
    except TimeoutError:
        logger.error(
            "delete_job cancellation timed out job_id=%s timeout_seconds=%s",
            flow_run_id,
            settings.cancellation_timeout_seconds,
        )
        return CancellationWaitResult.FAILED
    except Exception:
        logger.exception("delete_job failed while waiting for cancellation job_id=%s", flow_run_id)
        return CancellationWaitResult.FAILED


async def _delete_flow_run(
    flow_run_id: UUID,
    job_name: str,
    workflow_type: str,
    user_name: str,
) -> bool:
    try:
        deleted = await delete_run(flow_run_id)
    except Exception:
        logger.exception("delete_job failed to delete job_id=%s", flow_run_id)
        return False

    if not deleted:
        logger.error("delete_job failed to delete job_id=%s: flow run was not found", flow_run_id)
        return False

    logger.info(
        "delete_job status=DELETED job_name=%s workflow_type=%s user_name=%s",
        job_name,
        workflow_type,
        user_name,
    )
    return True


async def _cleanup_stored_resources(flow_run_id: UUID, resources: list[JobCleanupResource], store: JobStore) -> bool:
    succeeded = True
    for stored_resource in resources:
        resource_deleted = cleanup_resources([stored_resource.resource], settings) is not False
        error = None if resource_deleted else "Resource cleanup failed; see orchestrator logs"
        await store.record_cleanup_result(flow_run_id, stored_resource.resource_key, error)
        succeeded = succeeded and resource_deleted
    return succeeded


async def _finalize_job_deletion(
    flow_run_id: UUID,
    job_name: str,
    workflow_type: str,
    user_name: str,
    store: JobStore,
    *,
    prefect_history_missing: bool = False,
) -> bool:
    resources = await store.get_cleanup_resources(flow_run_id)
    await _cleanup_stored_resources(flow_run_id, resources, store)
    if not prefect_history_missing and not await _delete_flow_run(flow_run_id, job_name, workflow_type, user_name):
        return False
    await store.mark_job_deleted(flow_run_id)
    return True


async def _finish_flow_run_deletion_after_cancellation(flow_run_id: UUID, store: JobStore) -> None:
    try:
        wait_result = await _wait_for_flow_run_cancellation(flow_run_id)
        if wait_result == CancellationWaitResult.FAILED:
            return
        job = await store.get_job(flow_run_id)
        if job is None:
            logger.error("delete_job background deletion failed job_id=%s: job was not found", flow_run_id)
            return
        if await _finalize_job_deletion(
            flow_run_id,
            job.job_name,
            job.workflow_type,
            job.user_name,
            store,
            prefect_history_missing=wait_result == CancellationWaitResult.MISSING,
        ):
            await store.update_job_status(flow_run_id, JobStatus.CANCELLED)
    except Exception:
        logger.exception("delete_job background deletion failed job_id=%s", flow_run_id)


@router.post("/", response_model=JobStatusResponse)
async def create_job(job_input: JobInput, store: JobStoreDependency) -> JobStatusResponse:
    """Start new job: 'input_params_dict' can have lists and (nested) dicts as values."""
    workflow_definition = await workflow_registry.get_workflow_definition(job_input.workflow_type)
    run_tags: list[str] = []
    if job_input.workflow_type:
        run_tags.append(f"type:{job_input.workflow_type}")
    if job_input.user_name:
        run_tags.append(f"user:{job_input.user_name}")

    input_esdl = _decode_input_esdl(job_input.input_esdl)
    flow_results_folder = _create_flow_results_folder(job_input.job_name)
    input_esdl_reference, input_esdl_resource = _store_input_esdl(input_esdl, flow_results_folder)
    parameters = {
        "input_esdl_minio_path": input_esdl_reference,
        "flow_results_folder": flow_results_folder,
        "workflow_type_name": job_input.workflow_type,
        "workflow_config": job_input.input_params_dict,
    }

    try:
        run_id = await trigger_flow_run(
            run_name=job_input.job_name,
            deployment_base_name=workflow_definition.prefect_flow_name,
            deployment_version=job_input.version,
            parameters=parameters,
            run_tags=run_tags,
            memory_limit=workflow_definition.memory_limit,
        )
    except RuntimeError as exc:
        _cleanup_unregistered_input_esdl(input_esdl_resource, "Prefect trigger failure")
        raise_for_prefect_runtime_error(exc)
        raise
    except Exception:
        _cleanup_unregistered_input_esdl(input_esdl_resource, "Prefect trigger failure")
        raise

    await _persist_created_job(run_id, job_input, input_esdl_resource, store)

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
async def list_jobs(store: JobStoreDependency) -> list[JobSummary]:
    """Return a summary of all jobs."""
    try:
        flow_runs = await get_runs()
    except RuntimeError as exc:
        raise_for_prefect_runtime_error(exc)
        raise
    for run in flow_runs:
        if run.state is None:
            continue

        run_status = from_prefect_state_type_to_job_status(run.state.type)
        stored_job = await store.get_job(run.id)
        if stored_job is not None and stored_job.status != run_status:
            await store.update_job_status(run.id, run_status)

    return [
        JobSummary(
            job_id=stored_job.job_id,
            job_name=stored_job.job_name,
            status=stored_job.status,
            user_name=stored_job.user_name,
            project_name="",
        )
        for stored_job in await store.list_jobs()
    ]


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, store: JobStoreDependency) -> JobResponse:
    """Return job details."""
    try:
        job_uuid = UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format") from None

    stored_job = await store.get_job(job_uuid)
    if stored_job is None or stored_job.deleted_at is not None:
        raise HTTPException(status_code=404, detail=f"Unknown job {job_id}")
    try:
        run_name, state_type, input_parameters, tags, artifacts, logs = await get_flow_run_status_and_results(
            job_uuid, settings.minio_host, settings.minio_port, settings.minio_access_key, settings.minio_secret
        )
    except ObjectNotFound:
        return _job_response_without_prefect(stored_job)
    except RuntimeError as exc:
        raise_for_prefect_runtime_error(exc)
        raise
    status = from_prefect_state_type_to_job_status(state_type)
    if stored_job.status != status:
        await store.update_job_status(job_uuid, status)

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

    input_esdl = _read_input_esdl(input_parameters.get("input_esdl_minio_path"))

    return JobResponse(
        job_id=job_uuid,
        job_name=stored_job.job_name,
        status=status,
        user_name=stored_job.user_name,
        workflow_type=stored_job.workflow_type,
        input_esdl=_b64_encode_esdl_str(input_esdl),
        output_esdl=_b64_encode_esdl_str(output_esdl_data),
        input_params_dict=input_parameters.get("workflow_config", {}),
        timeout_after_s=input_parameters.get("timeout_after_s", 3600),
        job_priority=input_parameters.get("job_priority"),
        esdl_feedback=_get_esdl_feedback(esdl_messages_data),
        progress_fraction=progress_artifact.get("data") if progress_artifact else None,
        progress_message=progress_artifact.get("description") if progress_artifact else None,
        logs=logs,
    )


def _job_response_without_prefect(job: Job) -> JobResponse:
    """Return durable job data when Prefect history has already been removed."""
    return JobResponse(
        job_id=job.job_id,
        job_name=job.job_name,
        status=job.status,
        user_name=job.user_name,
        workflow_type=job.workflow_type,
        input_esdl=None,
        timeout_after_s=3600,
        logs="",
        esdl_feedback=[],
    )


@router.post("/{job_id}/cleanup-resources", status_code=status.HTTP_204_NO_CONTENT)
async def register_job_cleanup_resources(
    job_id: str,
    cleanup_resources_payload: JobCleanupResources,
    store: JobStoreDependency,
) -> None:
    """Durably register resources that must be removed when a job is deleted."""
    try:
        job_uuid = UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format") from None
    if await store.get_job(job_uuid) is None:
        raise HTTPException(status_code=404, detail=f"Unknown job {job_id}")
    await store.register_cleanup_resources(job_uuid, cleanup_resources_payload.resources)


@router.delete("/{job_id}", response_model=JobDeleteResponse)
async def delete_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    response: Response,
    store: JobStoreDependency,
) -> JobDeleteResponse:
    """Cancel a job and remove its Prefect history once it has stopped."""
    try:
        job_uuid = UUID(job_id)
    except ValueError:
        logger.error("delete_job failed: invalid job_id=%s", job_id)
        return JobDeleteResponse(job_id=None, deleted=False)

    stored_job = await store.get_job(job_uuid)
    if stored_job is None:
        logger.error("delete_job failed job_id=%s: job was not found in the orchestrator database", job_uuid)
        return JobDeleteResponse(job_id=job_uuid, deleted=False)

    try:
        async with get_client() as client:
            try:
                flow_run = await client.read_flow_run(job_uuid)
            except ObjectNotFound:
                flow_run = None

            if flow_run is not None and flow_run.state is not None and not flow_run.state.is_final():
                cancellation_result = await client.set_flow_run_state(job_uuid, Cancelling())
                if cancellation_result.status != SetStateStatus.ACCEPT:
                    try:
                        flow_run = await client.read_flow_run(job_uuid)
                    except ObjectNotFound:
                        flow_run = None

                if flow_run is not None and flow_run.state is not None and not flow_run.state.is_final():
                    if (
                        cancellation_result.status != SetStateStatus.ACCEPT
                        and flow_run.state.type != StateType.CANCELLING
                    ):
                        logger.error(
                            "Prefect did not accept cancellation for job %s status=%s current_state=%s",
                            job_uuid,
                            cancellation_result.status,
                            flow_run.state.type.name,
                        )
                        return JobDeleteResponse(job_id=job_uuid, deleted=False)
                    background_tasks.add_task(_finish_flow_run_deletion_after_cancellation, job_uuid, store)
                    response.status_code = status.HTTP_202_ACCEPTED
                    return JobDeleteResponse(job_id=job_uuid, deleted=False)

        if flow_run is not None and flow_run.state is None:
            logger.error("delete_job failed job_id=%s: flow run has no Prefect state", job_uuid)

        tags_by_key = _get_tags_by_key(flow_run.tags) if flow_run is not None else {}
        job_name = (flow_run.name or stored_job.job_name) if flow_run is not None else stored_job.job_name
        deleted = await _finalize_job_deletion(
            job_uuid,
            job_name,
            tags_by_key.get("type", stored_job.workflow_type),
            tags_by_key.get("user", stored_job.user_name),
            store,
            prefect_history_missing=flow_run is None,
        )
        return JobDeleteResponse(job_id=job_uuid, deleted=deleted)
    except Exception:
        logger.exception("delete_job failed job_id=%s", job_uuid)
        return JobDeleteResponse(job_id=job_uuid, deleted=False)
