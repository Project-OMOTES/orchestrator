import logging
import signal
import sys
import threading
import pprint
import uuid
from datetime import timedelta
from types import FrameType
from typing import Any, Union

from omotes_orchestrator.postgres_interface import PostgresInterface
from omotes_orchestrator.postgres_job_manager import PostgresJobManager
from omotes_sdk.internal.orchestrator_worker_events.messages.task_pb2 import (
    TaskResult,
    TaskProgressUpdate,
)
from omotes_sdk_protocol.job_pb2 import (
    JobSubmission,
    JobResult,
    JobStatusUpdate,
    JobProgressUpdate,
    JobCancel,
)
from omotes_sdk_protocol.workflow_pb2 import RequestAvailableWorkflows
from omotes_sdk.workflow_type import WorkflowTypeManager
from omotes_sdk.job import Job

from google.protobuf import json_format

from omotes_orchestrator.celery_interface import CeleryInterface
from omotes_orchestrator.config import OrchestratorConfig
from omotes_orchestrator.db_models.job import JobStatus as JobStatusDB, JobDB
from omotes_orchestrator.sdk_interface import SDKInterface
from omotes_orchestrator.timeout_job_manager import TimeoutJobManager
from omotes_orchestrator.worker_interface import WorkerInterface

logger = logging.getLogger("omotes_orchestrator")


class BarrierTimeoutException(BaseException):
    """Exception which is thrown if the barrier is not ready within the specified time."""

    pass


class MissingBarrierException(BaseException):
    """Exception which is thrown if a barrier is waited on or set while the barrier is missing."""

    pass


class LifeCycleBarrierManager:
    """Maintain a (processing) barrier per job until a lifecycle is finished.

    Is used currently to prevent processing of status, progress and result updates until
    a job is completely submitted. Will block any thread that calls `wait_for_barrier` until
    the barrier is set.
    """

    BARRIER_WAIT_TIMEOUT = 2.0
    """How long a thread may wait for the barrier in seconds."""

    _barrier_modification_lock: threading.Lock
    """Lock required before making modifications to `_barriers`."""
    _barriers: dict[uuid.UUID, threading.Event]
    """A dictionary of all barriers by id."""

    def __init__(self) -> None:
        """Construct the lifecycle barrier manager."""
        self._barrier_modification_lock = threading.Lock()
        self._barriers = {}

    def ensure_barrier(self, job_id: uuid.UUID) -> threading.Event:
        """Ensure that a barrier is available for the job with `job_id`.

        :param: job_id: The id of the job to ensure that a barrier is available.
        :return: The barrier that is either created or was already available.
        """
        # First check if the barrier doesn't exist before waiting on the modification lock
        if job_id not in self._barriers:
            # Barrier doesn't exist yet, queue for the lock to add the barrier.
            with self._barrier_modification_lock:
                # Gained access to modify the dict but in the meantime someone else may have added
                # the barrier. So check again to guarantee this is the only thread to add the
                # barrier.
                if job_id not in self._barriers:
                    self._barriers[job_id] = threading.Event()

        return self._barriers[job_id]

    def _get_barrier(self, job_id: uuid.UUID) -> threading.Event:
        """Retrieve the barrier and throw exception if the barrier is not available."""
        if job_id not in self._barriers:
            raise MissingBarrierException(f"Lifecycle barrier is missing for job {job_id}")
        return self._barriers[job_id]

    def set_barrier(self, job_id: uuid.UUID) -> None:
        """Set the barrier for the job to ready.

        Any threads that were waiting are notified they can continue and future threads will not
        wait.

        :param job_id: The id of the job for which the barrier may be set to ready.
        :raises MissingBarrierException: Thrown if the barrier is not ensured or already cleaned up.
        """
        barrier = self._get_barrier(job_id)
        barrier.set()

    def wait_for_barrier(self, job_id: uuid.UUID) -> None:
        """Wait until the barrier for the job is ready.

        May wait up to `BARRIER_WAIT_TIMEOUT` seconds

        :param job_id: The id of the job for which to wait until the barrier is ready.
        :raises BarrierTimeoutException: If the barrier is not ready within BARRIER_WAIT_TIMEOUT,
            this exception is thrown.
        :raises MissingBarrierException: Thrown if the barrier is not ensured or already cleaned up.
        """
        barrier = self._get_barrier(job_id)
        result = barrier.wait(LifeCycleBarrierManager.BARRIER_WAIT_TIMEOUT)
        if not result:
            raise BarrierTimeoutException(f"Barrier for job {job_id} was not ready on time.")

    def cleanup_barrier(self, job_id: uuid.UUID) -> None:
        """Remove the barrier from memory.

        This function should be called when no more threads will ever need the lifecycle barrier
        anymore. If no barrier exists for `job_id` nothing is done so it is safe to call it
        multiple times.

        :param job_id: The id of the job for which the barrier should be cleaned up.
        """
        with self._barrier_modification_lock:
            if job_id in self._barriers:
                del self._barriers[job_id]


class Orchestrator:
    """Orchestrator application."""

    config: OrchestratorConfig
    """Configuration parameters for Orchestrator application."""
    omotes_sdk_if: SDKInterface
    """Interface to OMOTES SDK from orchestrator-side."""
    worker_if: WorkerInterface
    """Interface to RabbitMQ, Celery side for events and results send by workers outside of
    Celery."""
    celery_if: CeleryInterface
    """Interface to the Celery app."""
    postgresql_if: PostgresInterface
    """Interface to PostgreSQL."""
    workflow_manager: WorkflowTypeManager
    """Store for all available workflow types."""
    postgres_job_manager: PostgresJobManager
    """Manage and clean up the postgres stale job row."""
    timeout_job_manager: TimeoutJobManager
    """Cancel and delete the job when it is timed out."""
    _init_barriers: LifeCycleBarrierManager

    def __init__(
        self,
        config: OrchestratorConfig,
        omotes_orchestrator_sdk_if: SDKInterface,
        worker_if: WorkerInterface,
        celery_if: CeleryInterface,
        postgresql_if: PostgresInterface,
        workflow_manager: WorkflowTypeManager,
        postgres_job_manager: PostgresJobManager,
        timeout_job_manager: TimeoutJobManager,
    ):
        """Construct the orchestrator.

        :param config: Configuration parameters for Orchestrator application.
        :param omotes_orchestrator_sdk_if: Interface to OMOTES SDK.
        :param worker_if: Interface to RabbitMQ, Celery side for events and results send by workers
            outside of Celery.
        :param celery_if: Interface to the Celery app.
        :param postgresql_if: Interface to PostgreSQL to persist job information.
        :param workflow_manager: Store for all available workflow types.
        :param postgres_job_manager: Manage and clean up postgres stale job rows.
        :param timeout_job_manager: Cancel and delete the job when it is timed out.
        """
        self.config = config
        self.omotes_sdk_if = omotes_orchestrator_sdk_if
        self.worker_if = worker_if
        self.celery_if = celery_if
        self.postgresql_if = postgresql_if
        self.workflow_manager = workflow_manager
        self.postgres_job_manager = postgres_job_manager
        self.timeout_job_manager = timeout_job_manager
        self._init_barriers = LifeCycleBarrierManager()

        if self.timeout_job_manager.orchestrator is None:
            self.timeout_job_manager.orchestrator = self
            logger.info("Assigned orchestrator %s to the timeout job manager.", self)

    def _resume_init_barriers(self, all_jobs: list[JobDB]) -> None:
        """Resume the INIT lifecycle barriers for all jobs while starting the orchestrator.

        :param all_jobs: All jobs that are known while the orchestrator is starting up.
        """
        for job in all_jobs:
            if job.status != JobStatusDB.REGISTERED:
                self._init_barriers.ensure_barrier(job.job_id)
                self._init_barriers.set_barrier(job.job_id)

    def start(self) -> None:
        """Start the orchestrator."""
        self.postgresql_if.start()
        self._resume_init_barriers(self.postgresql_if.get_all_jobs())

        self.celery_if.start()

        self.worker_if.start()
        self.worker_if.connect_to_worker_task_results(
            callback_on_worker_task_result=self.task_result_received
        )
        self.worker_if.connect_to_worker_task_progress_updates(
            callback_on_worker_task_progress_update=self.task_progress_update
        )

        self.omotes_sdk_if.start()
        self.omotes_sdk_if.connect_to_job_submissions(
            callback_on_new_job=self.new_job_submitted_handler
        )
        self.omotes_sdk_if.connect_to_job_cancellations(
            callback_on_job_cancel=self.job_cancellation_handler
        )

        self.omotes_sdk_if.connect_to_request_available_workflows(
            callback_on_request_workflows=self.request_workflows_handler
        )
        self.omotes_sdk_if.send_available_workflows()

        self.postgres_job_manager.start()
        self.timeout_job_manager.start()

    def stop(self) -> None:
        """Stop the orchestrator."""
        self.omotes_sdk_if.stop()
        self.worker_if.stop()
        self.celery_if.stop()
        self.postgres_job_manager.stop()
        self.timeout_job_manager.stop()
        self.postgresql_if.stop()

    def request_workflows_handler(self, request_workflows: RequestAvailableWorkflows) -> None:
        """When an available work flows request is received from the SDK.

        :param request_workflows: Request available work flows.
        """
        logger.info("Received an available workflows request")
        self.omotes_sdk_if.send_available_workflows()

    def new_job_submitted_handler(self, job_submission: JobSubmission) -> None:
        """When a new job is submitted through OMOTES SDK.

        Note: This function must be idempotent. It should submit a task to Celery and register it
        as such in the database. Only when the whole function is successful, do we consider
        the task to have been successfully submitted. It tries to (best effort) cancel the Celery
        task if the function is not successful but there are edge cases where this fails.
        In other words, next steps in the orchestrator have to deal with the fact that a Celery task
        is running but this function has not been successful.

        :param job_submission: Job submission message.
        """
        workflow_type = self.workflow_manager.get_workflow_by_name(job_submission.workflow_type)
        job_uuid = uuid.UUID(job_submission.uuid)
        if workflow_type is None:
            logger.warning(
                "Received a new job (id %s, reference %s) with unknown workflow type %s. "
                "Ignoring job.",
                job_submission.uuid,
                job_submission.job_reference,
                job_submission.workflow_type,
            )
            self.omotes_sdk_if.send_job_result_by_job_id(
                job_uuid=job_uuid,
                result=JobResult(
                    uuid=job_submission.uuid,
                    result_type=JobResult.ResultType.ERROR,
                    output_esdl=None,
                    logs=f"Workflow type {job_submission.workflow_type} is unknown by orchestrator "
                    f"at the time this submission was received.",
                ),
            )
            return

        job = Job(job_uuid, workflow_type)

        logger.info(
            "Received new job %s with reference %s for workflow type %s",
            job.id,
            job_submission.job_reference,
            job_submission.workflow_type,
        )
        submitted_job_id = uuid.UUID(job_submission.uuid)

        if self.postgresql_if.job_exists(submitted_job_id):
            # This case can happen when something wrong happened during new_job_submitted_handler
            # but the insert in SQL happened.
            status = self.postgresql_if.get_job_status(submitted_job_id)
            submit = status == JobStatusDB.REGISTERED

            logger.warning(
                "New job %s was already registered previously. Will be submitted %s", job.id, submit
            )
        else:
            logger.debug("New job %s was not yet registered. Registering and submitting.", job.id)

            if not job_submission.HasField("timeout_ms"):
                timeout_after_ms = None
                logger.debug(
                    "Timeout_ms is unset for new job, registering timeout_after_ms as null."
                )
            else:
                timeout_after_ms = timedelta(milliseconds=job_submission.timeout_ms)

            self.postgresql_if.put_new_job(
                job_id=job.id,
                workflow_type=job_submission.workflow_type,
                timeout_after=timeout_after_ms,
            )
            submit = True

        if submit:
            job_reference = None
            if job_submission.HasField("job_reference"):
                job_reference = job_submission.job_reference

            self._init_barriers.ensure_barrier(submitted_job_id)
            celery_task_id = self.celery_if.start_workflow(
                job.workflow_type,
                job.id,
                job_reference,
                job_submission.esdl,
                json_format.MessageToDict(job_submission.params_dict),
            )

            self.postgresql_if.set_job_submitted(job.id, celery_task_id)
            logger.debug(
                "New job %s with reference %s has been submitted.",
                job.id,
                job_submission.job_reference,
            )
            self._init_barriers.set_barrier(submitted_job_id)

    def job_cancellation_handler(self, job_cancellation: JobCancel) -> None:
        """When a cancellation request is received from the SDK.

        Note: This function must be idempotent. It will cancel the Celery task,
        remove the job from the database and send the last status update and result that the job
        is cancelled.

        If the job is registered in the database but no celery id is persisted, this is logged
        as a warning. Rationale: The queue through which cancellations are received is a different
        queue from where workers post results. Therefore, this function cannot rely on any ordering
        of when a job is submitted, a progress update is received or when a result is received.
        In other words, cancellations are only possible when the cancellation is received when the
        job is submitted or active. In all other cases, the cancellation is ignored.

        :param job_cancellation: Request to cancel a job.
        """
        logger.info("Received job cancellation for job %s", job_cancellation.uuid)
        job_id = uuid.UUID(job_cancellation.uuid)

        job_db = self.postgresql_if.get_job(job_id)
        if job_db and job_db.status == JobStatusDB.REGISTERED:
            self._init_barriers.wait_for_barrier(job_id)
            job_db = self.postgresql_if.get_job(job_id)

        if job_db is None:
            logger.warning(
                "Received a request to cancel job %s but it was already completed, "
                "cancelled, removed or was not yet submitted.",
                job_cancellation.uuid,
            )
        elif job_db.celery_id is None:
            logger.warning(
                "Received a request to cancel job %s but this was has been"
                "registered but not yet successfully submitted to Celery."
                "Ignoring message as it cannot be handled.",
                job_cancellation.uuid,
            )
        else:
            workflow_type = self.workflow_manager.get_workflow_by_name(job_db.workflow_type)

            if workflow_type is None:
                logger.error(
                    "Received a request to cancel job %s but workflow %s persisted in "
                    "database is not configured in this orchestrator.",
                    job_cancellation.uuid,
                    job_db.workflow_type,
                )
                # TODO Send an error result to SDK
            else:
                job = Job(id=job_db.job_id, workflow_type=workflow_type)

                self.celery_if.cancel_workflow(job_db.celery_id)
                self.omotes_sdk_if.send_job_status_update(
                    job=job,
                    status_update=JobStatusUpdate(
                        uuid=str(job.id), status=JobStatusUpdate.JobStatus.CANCELLED
                    ),
                )
                self.omotes_sdk_if.send_job_result(
                    job=job,
                    result=JobResult(
                        uuid=str(job.id),
                        result_type=JobResult.ResultType.CANCELLED,
                        output_esdl=None,
                        logs="",
                    ),
                )
                self._cleanup_job(job_id)

    def _cleanup_job(self, job_id: uuid.UUID) -> None:
        """Cleanup any references to job with id `job_id`.

        :param job_id: The job to clean up after.
        """
        self.postgresql_if.delete_job(job_id)
        self._init_barriers.cleanup_barrier(job_id)

    def task_result_received(self, task_result: TaskResult) -> None:
        """When a task result is received from a worker through RabbitMQ, Celery side.

        Note: This function must be idempotent.
        It may happen that the task is started multiple times when the job is submitted
        due to various reasons. In such a case, we consider only the job that is completely
        successfully submitted to be relevant. This means that the Celery task ID is equal
        to the one available in the SQL database.

        :param task_result: Protobuf `TaskResult` message.
        """
        workflow_type = self.workflow_manager.get_workflow_by_name(task_result.celery_task_type)
        if workflow_type is None:
            logger.error(
                "Received a result for %s but celery task %s is not configured in this"
                "orchestrator as a possible workflow.",
                task_result.job_id,
                task_result.celery_task_type,
            )
            # TODO Send an error result to SDK
        else:
            job = Job(
                id=uuid.UUID(task_result.job_id),
                workflow_type=workflow_type,
            )

            job_db = self.postgresql_if.get_job(job.id)
            if job_db and job_db.status == JobStatusDB.REGISTERED:
                self._init_barriers.wait_for_barrier(job.id)
                job_db = self.postgresql_if.get_job(job.id)

            # Confirm the job is still relevant.
            if job_db is None:
                logger.info("Ignoring result as job %s was already cancelled or completed.", job.id)
            elif job_db.celery_id != task_result.celery_task_id:
                logger.warning(
                    "Job %s has a result but it is not from the celery task that was expected."
                    "Ignoring result. Expected celery task id %s but received celery task id %s",
                    job.id,
                    job_db.celery_id,
                    task_result.celery_task_id,
                )
            elif task_result.result_type == TaskResult.ResultType.SUCCEEDED:
                logger.info(
                    "Received succeeded result for job %s through task %s",
                    task_result.job_id,
                    task_result.celery_task_id,
                )
                self.omotes_sdk_if.send_job_result(
                    job=job,
                    result=JobResult(
                        uuid=str(job.id),
                        result_type=JobResult.ResultType.SUCCEEDED,
                        output_esdl=task_result.output_esdl,
                        logs=task_result.logs,
                    ),
                )
                self._cleanup_job(job.id)
            elif task_result.result_type == TaskResult.ResultType.ERROR:
                logger.info(
                    "Received error result for job %s through task %s",
                    task_result.job_id,
                    task_result.celery_task_id,
                )
                self.omotes_sdk_if.send_job_result(
                    job=job,
                    result=JobResult(
                        uuid=str(job.id),
                        result_type=JobResult.ResultType.ERROR,
                        output_esdl=task_result.output_esdl,
                        logs=task_result.logs,
                    ),
                )
                self._cleanup_job(job.id)
            else:
                logger.error(
                    "Unknown task result %s. Please report and/or implement.",
                    task_result.result_type,
                )

    def task_progress_update(self, progress_update: TaskProgressUpdate) -> None:
        """When a task event is received from a worker through RabbitMQ, Celery side.

        Note: This function must be idempotent.
        It may happen that the task is started multiple times when the job is submitted
        due to various reasons. In such a case, we consider only the job that is completely
        successfully submitted to be relevant. This means that the Celery task ID is equal
        to the one available in the SQL database.

        :param progress_update: Protobuf `TaskProgressUpdate` message.
        """
        workflow_type = self.workflow_manager.get_workflow_by_name(progress_update.celery_task_type)
        if workflow_type is None:
            logger.error(
                "Received a progress update for %s but celery task %s is not configured in "
                "this orchestrator as a possible workflow.",
                progress_update.job_id,
                progress_update.celery_task_type,
            )
        else:
            job = Job(
                id=uuid.UUID(progress_update.job_id),
                workflow_type=workflow_type,
            )

            job_db = self.postgresql_if.get_job(job.id)
            if job_db and job_db.status == JobStatusDB.REGISTERED:
                self._init_barriers.wait_for_barrier(job.id)
                job_db = self.postgresql_if.get_job(job.id)

            # Confirm the job is still relevant.
            if job_db is None:
                logger.info(
                    "Ignoring progress update as job %s was already cancelled or completed and "
                    "cancelling the task",
                    job.id,
                )
                self.celery_if.cancel_workflow(progress_update.celery_task_id)
                return
            elif job_db.celery_id != progress_update.celery_task_id:
                logger.warning(
                    "Job %s has a progress update but it is not from the celery task that was "
                    "expected. Ignoring result. Expected celery task id %s but received celery "
                    "task id %s. Cancelling celery task with id %s",
                    job.id,
                    job_db.celery_id,
                    progress_update.celery_task_id,
                    progress_update.celery_task_id,
                )
                self.celery_if.cancel_workflow(progress_update.celery_task_id)
                return

            if (
                progress_update.HasField("status")
                and progress_update.status == TaskProgressUpdate.START
            ):
                # Progress indicating calculation start
                logger.debug("Progress update was the first. Setting job %s to RUNNING", job.id)
                amount_of_starts = self.postgresql_if.count_job_starts(job.id)
                logger.debug("Job %s has started %s times previously.", job.id, amount_of_starts)
                if amount_of_starts >= self.config.delivery_limit_threshold_per_job:
                    logger.error(
                        "Job %s has been started too many times (limit %s). Cancelling.",
                        job.id,
                        self.config.delivery_limit_threshold_per_job,
                    )
                    self.celery_if.cancel_workflow(job_db.celery_id)
                    self.omotes_sdk_if.send_job_status_update(
                        job=job,
                        status_update=JobStatusUpdate(
                            uuid=str(job.id), status=JobStatusUpdate.JobStatus.CANCELLED
                        ),
                    )
                    self.omotes_sdk_if.send_job_result(
                        job=job,
                        result=JobResult(
                            uuid=str(job.id),
                            result_type=JobResult.ResultType.ERROR,
                            output_esdl=None,
                            logs="Job cannot be processed due to being retried the maximum "
                            "number of times.",
                        ),
                    )

                    self._cleanup_job(job.id)
                    return
                else:
                    self.postgresql_if.set_job_running(job.id)

                    self.omotes_sdk_if.send_job_status_update(
                        job=job,
                        status_update=JobStatusUpdate(
                            uuid=str(job.id),
                            status=JobStatusUpdate.JobStatus.RUNNING,
                        ),
                    )

            if progress_update.HasField("progress"):
                logger.debug(
                    "Sending progress update %s (msg: %s) for job %s",
                    progress_update.progress,
                    progress_update.message,
                    job.id,
                )
                self.omotes_sdk_if.send_job_progress_update(
                    job,
                    JobProgressUpdate(
                        uuid=str(job.id),
                        progress=progress_update.progress,
                        message=progress_update.message,
                    ),
                )


def main() -> None:
    """Main function which creates and starts the orchestrator.

    Waits indefinitely until the orchestrator stops.
    """
    config = OrchestratorConfig()
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Config:\n%s", pprint.pformat(config))

    workflow_type_manager = WorkflowTypeManager.from_json_config_file(config.workflow_config)
    orchestrator_if = SDKInterface(config.rabbitmq_omotes, workflow_type_manager)
    celery_if = CeleryInterface(config.celery_config)
    worker_if = WorkerInterface(config)
    postgresql_if = PostgresInterface(config.postgres_config)
    postgres_job_manager = PostgresJobManager(postgresql_if, config.postgres_job_manager_config)
    timeout_job_manager = TimeoutJobManager(postgresql_if, None, config.timeout_job_manager_config)

    orchestrator = Orchestrator(
        config,
        orchestrator_if,
        worker_if,
        celery_if,
        postgresql_if,
        workflow_type_manager,
        postgres_job_manager,
        timeout_job_manager,
    )

    stop_event = threading.Event()

    def _stop_by_signal(sig_num: int, sig_stackframe: Union[FrameType, None]) -> Any:
        orchestrator.stop()
        stop_event.set()

    signal.signal(signal.SIGINT, _stop_by_signal)
    signal.signal(signal.SIGTERM, _stop_by_signal)
    if sys.platform.startswith(("win32", "cygwin")):
        # ctrl-break key not working
        signal.signal(signal.SIGBREAK, _stop_by_signal)  # type: ignore[attr-defined]
    else:
        signal.signal(signal.SIGQUIT, _stop_by_signal)  # type: ignore[attr-defined]

    orchestrator.start()
    stop_event.wait()


if __name__ == "__main__":
    main()
