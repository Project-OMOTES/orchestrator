import logging
import signal
import sys
import threading
import pprint
import uuid
from datetime import timedelta
from types import FrameType
from typing import Any, Union

from dotenv import load_dotenv
from omotes_orchestrator.postgres_interface import PostgresInterface
from omotes_sdk.internal.orchestrator_worker_events.messages.task_pb2 import (
    TaskResult,
    TaskProgressUpdate,
)
from omotes_sdk.internal.orchestrator.orchestrator_interface import OrchestratorInterface
from omotes_sdk.internal.common.broker_interface import BrokerInterface as JobBrokerInterface
from omotes_sdk_protocol.job_pb2 import (
    JobSubmission,
    JobResult,
    JobStatusUpdate,
    JobProgressUpdate,
    JobCancel,
)
from omotes_sdk.job import Job
from omotes_sdk.workflow_type import WorkflowTypeManager, WorkflowType
from google.protobuf import json_format

from omotes_orchestrator.celery_interface import CeleryInterface
from omotes_orchestrator.config import OrchestratorConfig
from omotes_orchestrator.db_models.job import JobStatus as JobStatusDB

load_dotenv(verbose=True)
logger = logging.getLogger("omotes_orchestrator")


class Orchestrator:
    """Orchestrator application."""

    omotes_if: OrchestratorInterface
    """Interface to OMOTES SDK."""
    jobs_broker_if: JobBrokerInterface
    """Interface to RabbitMQ, Celery side for events and results send by workers outside of
    Celery."""
    celery_if: CeleryInterface
    """Interface to the Celery app."""
    postgresql_if: PostgresInterface
    """Interface to PostgreSQL."""
    workflow_manager: WorkflowTypeManager
    """Store for all available workflow types."""

    def __init__(
        self,
        omotes_orchestrator_if: OrchestratorInterface,
        jobs_broker_if: JobBrokerInterface,
        celery_if: CeleryInterface,
        postgresql_if: PostgresInterface,
        workflow_manager: WorkflowTypeManager,
    ):
        """Construct the orchestrator.

        :param omotes_orchestrator_if: Interface to OMOTES SDK.
        :param jobs_broker_if: Interface to RabbitMQ, Celery side for events and results send by
            workers outside of Celery.
        :param celery_if: Interface to the Celery app.
        :param postgresql_if: Interface to PostgreSQL to persist job information.
        :param workflow_manager: Store for all available workflow types.
        """
        self.omotes_if = omotes_orchestrator_if
        self.jobs_broker_if = jobs_broker_if
        self.celery_if = celery_if
        self.postgresql_if = postgresql_if
        self.workflow_manager = workflow_manager

    def start(self) -> None:
        """Start the orchestrator."""
        self.postgresql_if.start()
        self.celery_if.start()
        self.omotes_if.start()
        self.omotes_if.connect_to_job_submissions(
            callback_on_new_job=self.new_job_submitted_handler
        )
        self.omotes_if.connect_to_job_cancellations(self.job_cancellation_handler)
        self.jobs_broker_if.start()
        self.jobs_broker_if.add_queue_subscription(
            "omotes_task_result_events", self.task_result_received
        )
        self.jobs_broker_if.add_queue_subscription(
            "omotes_task_progress_events", self.task_progress_update
        )

    def stop(self) -> None:
        """Stop the orchestrator."""
        self.omotes_if.stop()
        self.jobs_broker_if.stop()
        self.celery_if.stop()
        self.postgresql_if.stop()

    def new_job_submitted_handler(self, job_submission: JobSubmission, job: Job) -> None:
        """When a new job is submitted through OMOTES SDK.

        Note: This function must be idempotent. It should submit a task to Celery and register it
        as such in the database. Only when the whole function is successful, do we consider
        the task to have been successfully submitted. It tries to (best effort) cancel the Celery
        task if the function is not successful but there are edge cases where this fails.
        In other words, next steps in the orchestrator have to deal with the fact that a Celery task
        is running but this function has not been successful.

        :param job_submission: Job submission message.
        :param job: Reference to the submitted job.
        """
        logger.info(
            "Received new job %s for workflow type %s", job.id, job_submission.workflow_type
        )

        if self.postgresql_if.job_exists(job_submission.uuid):
            # This case can happen when something wrong happened during new_job_submitted_handler
            # but the insert in SQL happened.
            status = self.postgresql_if.get_job_status(job_submission.uuid)
            submit = status == JobStatusDB.REGISTERED

            logger.warning(
                "New job %s was already registered previously. Will be submitted %s", job.id, submit
            )
        else:
            logger.debug("New job %s was not yet registered. Registering and submitting.")
            self.postgresql_if.put_new_job(
                job_id=job.id,
                workflow_type=job_submission.workflow_type,
                timeout_after=timedelta(milliseconds=job_submission.timeout_ms),
            )
            submit = True

        if submit:
            celery_task_id = self.celery_if.start_workflow(
                job.workflow_type,
                job.id,
                job_submission.esdl,
                json_format.MessageToDict(job_submission.params_dict),
            )

            self.postgresql_if.set_job_submitted(job.id, celery_task_id)
            logger.debug("New job %s has been submitted.", job.id)

    def job_cancellation_handler(self, job_cancellation: JobCancel) -> None:
        logger.info("Received job cancellation for job %s", job_cancellation.uuid)

        job_db = self.postgresql_if.get_job(job_cancellation.uuid)

        if job_db is None:
            logger.warning(
                "Received a request to cancel job %s but it was already completed, "
                "cancelled or removed.",
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
            job = Job(id=job_db.job_id, workflow_type=workflow_type)

            self.celery_if.cancel_workflow(job_db.celery_id)
            self.omotes_if.send_job_status_update(
                job=job,
                status_update=JobStatusUpdate(
                    uuid=str(job.id), status=JobStatusUpdate.JobStatus.CANCELLED
                ),
            )
            self.omotes_if.send_job_result(
                job=job,
                result=JobResult(
                    uuid=str(job.id),
                    result_type=JobResult.ResultType.CANCELLED,
                    output_esdl=None,
                    logs="",
                ),
            )
            self.postgresql_if.delete_job(job.id)

    def task_result_received(self, serialized_message: bytes) -> None:
        """When a task result is received from a worker through RabbitMQ, Celery side.

        Note: This function must be idempotent.
        It may happen that the task is started multiple times when the job is submitted
        due to various reasons. In such a case, we consider only the job that is completely
        successfully submitted to be relevant. This means that the Celery task ID is equal
        to the one available in the SQL database.

        :param serialized_message: Protobuf encoded `TaskResult` message.
        """
        task_result = TaskResult()
        task_result.ParseFromString(serialized_message)
        logger.debug(
            "Received result for task %s (job %s) of type %s",
            task_result.celery_task_id,
            task_result.job_id,
            task_result.result_type,
        )
        workflow_type = self.workflow_manager.get_workflow_by_name(task_result.celery_task_type)
        job = Job(
            id=uuid.UUID(task_result.job_id),
            workflow_type=workflow_type,
        )

        job_db = self.postgresql_if.get_job(job.id)

        # Confirm the job is still relevant.
        if job_db is None:
            logger.info("Ignoring result as job %s was already cancelled or completed.", job.id)
        elif job_db.celery_id != task_result.celery_task_id:
            logger.warning(
                "Job %s has a result but was not successfully submitted yet." "Ignoring result.",
                job.id,
            )

        elif task_result.result_type == TaskResult.ResultType.SUCCEEDED:
            logger.info(
                "Received succeeded result for job %s through task %s",
                task_result.job_id,
                task_result.celery_task_id,
            )
            self.omotes_if.send_job_result(
                job=job,
                result=JobResult(
                    uuid=str(job.id),
                    result_type=JobResult.ResultType.SUCCEEDED,
                    output_esdl=task_result.output_esdl,
                    logs=task_result.logs,
                ),
            )
            self.postgresql_if.delete_job(job.id)

    def task_progress_update(self, serialized_message: bytes) -> None:
        """When a task event is received from a worker through RabbitMQ, Celery side.

        Note: This function must be idempotent.
        It may happen that the task is started multiple times when the job is submitted
        due to various reasons. In such a case, we consider only the job that is completely
        successfully submitted to be relevant. This means that the Celery task ID is equal
        to the one available in the SQL database.

        :param serialized_message: Protobuf encoded `TaskProgressUpdate` message.
        """
        progress_update = TaskProgressUpdate()
        progress_update.ParseFromString(serialized_message)
        logger.debug(
            "Received progress update for job %s (celery task id %s) to progress %s with message: "
            "%s",
            progress_update.job_id,
            progress_update.celery_task_id,
            progress_update.progress,
            progress_update.message,
        )

        workflow_type = self.workflow_manager.get_workflow_by_name(progress_update.celery_task_type)
        job = Job(
            id=uuid.UUID(progress_update.job_id),
            workflow_type=workflow_type,
        )

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
                "Job %s has a progress update but was not successfully submitted yet."
                "Ignoring progress update and cancelling this task.",
                job.id,
            )
            self.celery_if.cancel_workflow(progress_update.celery_task_id)
            return

        if progress_update.progress == 0:  # first progress indicating calculation start
            logger.debug("Progress update was the first. Setting job %s to RUNNING", job.id)
            self.omotes_if.send_job_status_update(
                job=job,
                status_update=JobStatusUpdate(
                    uuid=str(job.id),
                    status=JobStatusUpdate.JobStatus.RUNNING,
                ),
            )
            self.postgresql_if.set_job_running(job.id)
        logger.debug(
            "Sending progress update %s (msg: %s) for job %s",
            progress_update.progress,
            progress_update.message,
            job.id,
        )
        self.omotes_if.send_job_progress_update(
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

    workflow_type_manager = WorkflowTypeManager(
        possible_workflows=[
            WorkflowType(
                workflow_type_name="grow_optimizer", workflow_type_description_name="Grow Optimizer"
            ),
            WorkflowType(
                workflow_type_name="grow_simulator", workflow_type_description_name="Grow Simulator"
            ),
            WorkflowType(
                workflow_type_name="grow_optimizer_no_heat_losses",
                workflow_type_description_name="Grow Optimizer without heat losses",
            ),
            WorkflowType(
                workflow_type_name="grow_optimizer_no_heat_losses_discounted_capex",
                workflow_type_description_name="Grow Optimizer without heat losses and a "
                "discounted CAPEX",
            ),
            WorkflowType(
                workflow_type_name="simulator",
                workflow_type_description_name="High fidelity simulator",
            ),
        ]
    )
    orchestrator_if = OrchestratorInterface(config.rabbitmq_omotes, workflow_type_manager)
    celery_if = CeleryInterface(config.celery_config)
    jobs_broker_if = JobBrokerInterface(config.rabbitmq_worker_events)
    postgresql_if = PostgresInterface(config.postgres_config)
    orchestrator = Orchestrator(
        orchestrator_if, jobs_broker_if, celery_if, postgresql_if, workflow_type_manager
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
