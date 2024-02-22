import logging
import signal
import sys
import threading
import pprint
import uuid

from dotenv import load_dotenv
from omotes_sdk.internal.orchestrator_worker_events.messages.task_pb2 import (
    TaskResult,
    TaskProgressUpdate,
)
from omotes_sdk.internal.orchestrator.orchestrator_interface import OrchestratorInterface
from omotes_sdk.internal.common.broker_interface import BrokerInterface as JobBrokerInterface
from omotes_sdk_protocol.job_pb2 import JobSubmission, JobResult, JobStatusUpdate, JobProgressUpdate
from omotes_sdk.job import Job
from omotes_sdk.workflow_type import WorkflowTypeManager, WorkflowType

from omotes_orchestrator.celery_interface import CeleryInterface
from omotes_orchestrator.config import OrchestratorConfig


load_dotenv(verbose=True)
logger = logging.getLogger("omotes_orchestrator")


class Orchestrator:
    omotes_if: OrchestratorInterface
    jobs_broker_if: JobBrokerInterface
    celery_if: CeleryInterface

    def __init__(
        self,
        omotes_orchestrator_if: OrchestratorInterface,
        jobs_broker_if: JobBrokerInterface,
        celery_if: CeleryInterface,
    ):
        self.omotes_if = omotes_orchestrator_if
        self.jobs_broker_if = jobs_broker_if
        self.celery_if = celery_if

    def start(self):
        self.celery_if.start()
        self.omotes_if.start()
        self.omotes_if.connect_to_job_submissions(
            callback_on_new_job=self.new_job_submitted_handler
        )
        self.jobs_broker_if.start()
        self.jobs_broker_if.add_queue_subscription(
            "omotes_task_result_events", self.task_result_received
        )
        self.jobs_broker_if.add_queue_subscription(
            "omotes_task_progress_events", self.task_progress_update
        )

    def stop(self):
        self.omotes_if.stop()
        self.celery_if.stop()
        self.jobs_broker_if.stop()

    def new_job_submitted_handler(self, job_submission: JobSubmission, job: Job) -> None:
        logger.info(
            "Received new job %s for workflow type %s", job.id, job_submission.workflow_type
        )
        self.celery_if.start_workflow(job.workflow_type, job.id, job_submission.esdl)

    def task_result_received(self, serialized_message: bytes) -> None:
        task_result = TaskResult()
        task_result.ParseFromString(serialized_message)
        logger.debug(
            "Received result for task %s (job %s) of type %s",
            task_result.celery_task_id,
            task_result.job_id,
            task_result.result_type,
        )

        if task_result.result_type == TaskResult.ResultType.SUCCEEDED:
            job = Job(
                id=uuid.UUID(task_result.job_id),
                workflow_type=WorkflowType(task_result.celery_task_type, ""),
            )  # TODO Get workflow from WorkflowManager
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

    def task_progress_update(self, serialized_message: bytes) -> None:
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

        job = Job(
            id=uuid.UUID(progress_update.job_id),
            workflow_type=WorkflowType(progress_update.celery_task_type, ""),
        )  # TODO Get workflow from WorkflowManager

        if progress_update.progress == 0:  # first progress indicating calculation start
            self.omotes_if.send_job_status_update(
                job=job,
                status_update=JobStatusUpdate(
                    uuid=str(job.id),
                    status=JobStatusUpdate.JobStatus.RUNNING,
                ),
            )

        self.omotes_if.send_job_progress_update(
            job,
            JobProgressUpdate(
                uuid=str(job.id),
                progress=progress_update.progress,
                message=progress_update.message,
            ),
        )


def main():
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
                workflow_type_name="simulator",
                workflow_type_description_name="High fidelity simulator",
            ),
        ]
    )
    orchestrator_if = OrchestratorInterface(config.rabbitmq_omotes, workflow_type_manager)
    celery_if = CeleryInterface(config.celery_config)
    jobs_broker_if = JobBrokerInterface(config.rabbitmq_worker_events)
    orchestrator = Orchestrator(orchestrator_if, jobs_broker_if, celery_if)

    stop_event = threading.Event()

    def _stop_by_signal(sig_num, sig_stackframe):
        orchestrator.stop()
        stop_event.set()

    signal.signal(signal.SIGINT, _stop_by_signal)
    signal.signal(signal.SIGTERM, _stop_by_signal)
    if sys.platform.startswith(("win32", "cygwin")):
        signal.signal(signal.SIGBREAK, _stop_by_signal)  # ctrl-break key not working
    else:
        signal.signal(signal.SIGQUIT, _stop_by_signal)

    orchestrator.start()
    stop_event.wait()


if __name__ == "__main__":
    main()
