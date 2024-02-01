import logging
import pickle
import signal
import threading

import jsonpickle
from omotes_job_tools.messages import StatusUpdateMessage, TaskStatus, CalculationResult
from omotes_sdk_protocol.job_pb2 import JobSubmission, JobResult
from omotes_sdk.config import RabbitMQConfig
from omotes_sdk.job import Job
from omotes_sdk.orchestrator_interface import OrchestratorInterface
from omotes_sdk.workflow_type import WorkflowTypeManager, WorkflowType
from omotes_job_tools.broker_interface import BrokerInterface as JobBrokerInterface

from omotes_orchestrator.celery_interface import CeleryInterface, PostgreSQLConfig


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
        self.omotes_if.connect_to_job_submissions(callback_on_new_job=self.new_job_submitted_handler)
        self.jobs_broker_if.start()
        self.jobs_broker_if.add_queue_subscription("omotes_task_events", self.task_status_update)

    def stop(self):
        self.omotes_if.stop()
        self.celery_if.stop()
        self.jobs_broker_if.stop()

    def new_job_submitted_handler(self, job_submission: JobSubmission, job: Job) -> None:
        logger.info("Received new job %s for workflow type %s", job.id, job_submission.workflow_type)
        self.celery_if.start_workflow(job.workflow_type, job.id, job_submission.esdl)

    def task_status_update(self, serialized_message: bytes) -> None:
        status_update = StatusUpdateMessage.from_dict(pickle.loads(serialized_message))
        logger.debug(
            "Received task status update for task %s (job %s) and new status %s",
            status_update.celery_task_id,
            status_update.omotes_job_id,
            status_update.status,
        )
        if status_update.status == TaskStatus.SUCCEEDED:
            job = Job(
                id=status_update.omotes_job_id, workflow_type=WorkflowType(status_update.task_type, "")
            )  # TODO Get workflow from WorkflowManager
            result: CalculationResult = jsonpickle.decode(self.celery_if.retrieve_result(status_update.celery_task_id))
            logger.info(
                "Received result for job %s through task %s", status_update.omotes_job_id, status_update.celery_task_id
            )
            job_result_msg = JobResult(
                uuid=str(job.id),
                result_type=JobResult.ResultType.SUCCEEDED,
                success=JobResult.Succes(output_esdl=result.output_esdl.encode()),
            )
            self.omotes_if.send_job_result(job, job_result_msg)


def main():
    omotes_rabbitmq_config = RabbitMQConfig(username="omotes", password="somepass1", virtual_host="omotes")
    celery_rabbitmq_config = RabbitMQConfig(username="celery", password="somepass2", virtual_host="omotes_celery")
    celery_postgresql_config = PostgreSQLConfig(
        username="celery", password="somepass3", database="omotes_celery", host="localhost", port=5432
    )

    workflow_type_manager = WorkflowTypeManager(
        possible_workflows=[
            WorkflowType(workflow_type_name="grow_optimizer", workflow_type_description_name="Grow Optimizer")
        ]
    )
    orchestrator_if = OrchestratorInterface(omotes_rabbitmq_config, workflow_type_manager)
    celery_if = CeleryInterface(celery_rabbitmq_config, celery_postgresql_config)
    jobs_broker_if = JobBrokerInterface(celery_rabbitmq_config)
    orchestrator = Orchestrator(orchestrator_if, jobs_broker_if, celery_if)

    stop_event = threading.Event()

    def _stop_by_signal(sig_num, sig_stackframe):
        orchestrator.stop()
        stop_event.set()

    signal.signal(signal.SIGINT, _stop_by_signal)
    signal.signal(signal.SIGTERM, _stop_by_signal)
    signal.signal(signal.SIGQUIT, _stop_by_signal)

    orchestrator.start()
    stop_event.wait()


if __name__ == "__main__":
    main()
