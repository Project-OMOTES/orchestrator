import logging
from dataclasses import dataclass
from typing import Callable

from omotes_sdk.internal.common.broker_interface import BrokerInterface, AMQPQueueType
from omotes_sdk.internal.orchestrator_worker_events.messages.task_pb2 import (
    TaskResult,
    TaskProgressUpdate,
)

from omotes_orchestrator.config import OrchestratorConfig

logger = logging.getLogger("omotes_orchestrator")


@dataclass
class TaskResultHandler:
    """Handler to set up callback for receiving worker task results."""

    callback_on_task_result: Callable[[TaskResult], None]
    """Callback to call when a task result is received."""

    def callback_on_worker_task_result_wrapped(self, serialized_message: bytes) -> None:
        """Prepare the `TaskResult` message before passing them to the callback.

        :param serialized_message: Serialized AMQP message containing a task result.
        """
        task_result = TaskResult()
        task_result.ParseFromString(serialized_message)
        logger.debug(
            "Received result for task %s (job %s) of type %s",
            task_result.celery_task_id,
            task_result.job_id,
            task_result.result_type,
        )

        self.callback_on_task_result(task_result)


@dataclass
class TaskProgressUpdateHandler:
    """Handler to set up callback for receiving worker task progress updates."""

    callback_on_task_progress_update: Callable[[TaskProgressUpdate], None]
    """Callback to call when a task result is received."""

    def callback_on_worker_task_result_wrapped(self, serialized_message: bytes) -> None:
        """Prepare the `TaskProgressUpdate` message before passing them to the callback.

        :param serialized_message: Serialized AMQP message containing a task result.
        """
        progress_update = TaskProgressUpdate()
        progress_update.ParseFromString(serialized_message)

        if progress_update.HasField("progress"):
            logger.debug(
                "Received progress update for job %s (celery task id %s) to progress %s with "
                "message: %s",
                progress_update.job_id,
                progress_update.celery_task_id,
                progress_update.progress,
                progress_update.message,
            )
        if progress_update.HasField("status"):
            logger.debug(
                "Received progress update for job %s (celery task id %s) with status %s with "
                "message: %s",
                progress_update.job_id,
                progress_update.celery_task_id,
                progress_update.status,
                progress_update.message,
            )

        self.callback_on_task_progress_update(progress_update)


class WorkerInterface:
    """Connect to the Celery app which orchestrates the workers."""

    config: OrchestratorConfig
    """Configuration for Orchestrator application."""
    worker_broker_if: BrokerInterface
    """Interface to RabbitMQ, Celery side for events and results send by workers outside of
    Celery."""

    def __init__(self, config: OrchestratorConfig) -> None:
        """Create the interface to Celery.

        :param config: Configuration for Orchestrator application.
        """
        self.config = config
        self.worker_broker_if = BrokerInterface(config.rabbitmq_worker_events)

    def start(self) -> None:
        """Start the Celery app."""
        self.worker_broker_if.start()

    def stop(self) -> None:
        """Stop the Celery app."""
        self.worker_broker_if.stop()

    def connect_to_worker_task_results(
        self, callback_on_worker_task_result: Callable[[TaskResult], None]
    ) -> None:
        """Connect to the worker task results queue.

        :param callback_on_worker_task_result: Callback to handle results from the Celery workers.
        """
        callback_handler = TaskResultHandler(callback_on_worker_task_result)
        self.worker_broker_if.declare_queue_and_add_subscription(
            queue_name=self.config.task_result_queue_name,
            callback_on_message=callback_handler.callback_on_worker_task_result_wrapped,
            queue_type=AMQPQueueType.DURABLE,
        )

    def connect_to_worker_task_progress_updates(
        self, callback_on_worker_task_progress_update: Callable[[TaskProgressUpdate], None]
    ) -> None:
        """Connect to the worker progress updates queue.

        :param callback_on_worker_task_progress_update: Callback to handle progress updates from
            the Celery workers.
        """
        callback_handler = TaskProgressUpdateHandler(callback_on_worker_task_progress_update)
        self.worker_broker_if.declare_queue_and_add_subscription(
            queue_name=self.config.task_progress_queue_name,
            callback_on_message=callback_handler.callback_on_worker_task_result_wrapped,
            queue_type=AMQPQueueType.DURABLE,
        )
