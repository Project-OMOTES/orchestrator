import logging
import uuid
from dataclasses import dataclass
from typing import Callable

from omotes_sdk_protocol.job_pb2 import (
    JobSubmission,
    JobProgressUpdate,
    JobStatusUpdate,
    JobResult,
    JobDelete,
)
from omotes_sdk_protocol.workflow_pb2 import RequestAvailableWorkflows
from omotes_sdk.internal.common.broker_interface import BrokerInterface, AMQPQueueType
from omotes_sdk.config import RabbitMQConfig
from omotes_sdk.job import Job
from omotes_sdk.queue_names import OmotesQueueNames
from omotes_sdk.workflow_type import WorkflowTypeManager

logger = logging.getLogger("omotes_sdk_internal")


@dataclass
class JobSubmissionCallbackHandler:
    """Handler to setup and around callbacks associated with receiving a submitted job.

    A `JobSubmissionCallbackHandler` is created per job submission queue.
    """

    callback_on_new_job: Callable[[JobSubmission], None]
    """Callback to handle any jobs that are submitted."""

    def callback_on_new_job_wrapped(self, message: bytes) -> None:
        """Prepare the `Job` and `JobSubmission` messages before passing them to the callback.

        Expected workflow type is confirmed before passing it to handler.

        :param message: Serialized AMQP message containing a new job submission.
        """
        submitted_job = JobSubmission()
        submitted_job.ParseFromString(message)
        self.callback_on_new_job(submitted_job)


@dataclass
class JobDeletionHandler:
    """Handler to set up callback for receiving job deletions."""

    callback_on_delete_job: Callable[[JobDelete], None]
    """Callback to call when a deletion is received."""

    def callback_on_job_deleted_wrapped(self, message: bytes) -> None:
        """Prepare the `JobDelete` message before passing them to the callback.

        :param message: Serialized AMQP message containing a job deletion.
        """
        deleted_job = JobDelete()
        deleted_job.ParseFromString(message)

        self.callback_on_delete_job(deleted_job)


@dataclass
class RequestWorkflowsHandler:
    """Handler to set up callback for receiving available work flows requests."""

    callback_on_request_workflows: Callable[[RequestAvailableWorkflows], None]
    """Callback to call when a request work flows is received."""

    def callback_on_request_workflows_wrapped(self, message: bytes) -> None:
        """Prepare the `RequestAvailableWorkflows` message before passing them to the callback.

        :param message: Serialized AMQP message containing a request work flow.
        """
        request_available_workflows = RequestAvailableWorkflows()
        request_available_workflows.ParseFromString(message)

        self.callback_on_request_workflows(request_available_workflows)


class SDKInterface:
    """RabbitMQ interface specifically for the orchestrator."""

    broker_if: BrokerInterface
    """Interface to RabbitMQ."""
    workflow_type_manager: WorkflowTypeManager
    """All available workflow types."""

    def __init__(
        self, omotes_rabbitmq_config: RabbitMQConfig, workflow_type_manager: WorkflowTypeManager
    ):
        """Create the orchestrator interface.

        :param omotes_rabbitmq_config: How to connect to RabbitMQ as the orchestrator.
        :param workflow_type_manager: All available workflow types.
        """
        self.broker_if = BrokerInterface(omotes_rabbitmq_config)
        self.workflow_type_manager = workflow_type_manager

    def start(self) -> None:
        """Start the orchestrator interface."""
        self.broker_if.start()
        self.broker_if.declare_exchange(OmotesQueueNames.omotes_exchange_name())

    def stop(self) -> None:
        """Stop the orchestrator interface."""
        self.broker_if.stop()

    def connect_to_job_submissions(
        self, callback_on_new_job: Callable[[JobSubmission], None]
    ) -> None:
        """Connect to the job submission queue for each workflow type.

        :param callback_on_new_job: Callback to handle any new job submission.
        """
        callback_handler = JobSubmissionCallbackHandler(callback_on_new_job)
        self.broker_if.declare_queue_and_add_subscription(
            queue_name=OmotesQueueNames.job_submission_queue_name(),
            callback_on_message=callback_handler.callback_on_new_job_wrapped,
            queue_type=AMQPQueueType.DURABLE,
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
        )

    def connect_to_job_deletions(
        self, callback_on_job_delete: Callable[[JobDelete], None]
    ) -> None:
        """Connect to the job deletions queue.

        :param callback_on_job_delete: Callback to handle any new job deletions.
        """
        callback_handler = JobDeletionHandler(callback_on_job_delete)
        self.broker_if.declare_queue_and_add_subscription(
            queue_name=OmotesQueueNames.job_delete_queue_name(),
            callback_on_message=callback_handler.callback_on_job_deleted_wrapped,
            queue_type=AMQPQueueType.DURABLE,
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
        )

    def connect_to_request_available_workflows(
        self, callback_on_request_workflows: Callable[[RequestAvailableWorkflows], None]
    ) -> None:
        """Connect to the request available workflows queue.

        :param callback_on_request_workflows: Callback to handle workflow updates.
        """
        callback_handler = RequestWorkflowsHandler(callback_on_request_workflows)
        self.broker_if.declare_queue_and_add_subscription(
            queue_name=OmotesQueueNames.request_available_workflows_queue_name(),
            callback_on_message=callback_handler.callback_on_request_workflows_wrapped,
            queue_type=AMQPQueueType.EXCLUSIVE,
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
        )

    def send_job_progress_update(self, job: Job, progress_update: JobProgressUpdate) -> None:
        """Send a job progress update to the SDK.

        :param job: Job handle for which a progress update is send.
        :param progress_update: Current progress for the job.
        """
        self.broker_if.send_message_to(
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
            routing_key=OmotesQueueNames.job_progress_queue_name(job.id),
            message=progress_update.SerializeToString(),
        )

    def send_job_status_update(self, job: Job, status_update: JobStatusUpdate) -> None:
        """Send a job status update to the SDK.

        :param job: Job handle for which a status update is send.
        :param status_update: Current status for the job.
        """
        self.broker_if.send_message_to(
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
            routing_key=OmotesQueueNames.job_status_queue_name(job.id),
            message=status_update.SerializeToString(),
        )

    def send_job_result(self, job: Job, result: JobResult) -> None:
        """Send the job result to the SDK.

        :param job: Job to which the result belongs.
        :param result: The job result.
        """
        self.send_job_result_by_job_id(job.id, result)

    def send_job_result_by_job_id(self, job_uuid: uuid.UUID, result: JobResult) -> None:
        """Send the job result to the SDK.

        :param job_uuid: Identifier of the job to which the result belongs.
        :param result: The job result.
        """
        self.broker_if.send_message_to(
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
            routing_key=OmotesQueueNames.job_results_queue_name(job_uuid),
            message=result.SerializeToString(),
        )

    def send_available_workflows(self) -> None:
        """Send the available workflows to the SDK."""
        work_type_manager_pb = self.workflow_type_manager.to_pb_message()
        self.broker_if.send_message_to(
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
            routing_key=OmotesQueueNames.available_workflows_routing_key(),
            message=work_type_manager_pb.SerializeToString(),
        )
