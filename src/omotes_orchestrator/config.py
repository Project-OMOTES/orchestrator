import os
from dataclasses import dataclass

from omotes_sdk.internal.common.config import (
    RabbitMQConfig,
    EnvRabbitMQConfig,
)


@dataclass
class CeleryConfig:
    """Configuration class for Celery."""

    rabbitmq_config: RabbitMQConfig
    """Configuration to RabbitMQ as Celery app."""

    def __init__(self) -> None:
        """Construct the CeleryConfig."""
        self.rabbitmq_config = EnvRabbitMQConfig("CELERY_")


@dataclass
class OrchestratorConfig:
    """Configuration class for orchestrator."""

    celery_config: CeleryConfig
    """Configuration for Celery app."""
    rabbitmq_omotes: RabbitMQConfig
    """Configuration to connect to RabbitMQ on the OMOTES SDK side."""
    rabbitmq_worker_events: RabbitMQConfig
    """Configuration to connect to RabbitMQ on the Celery side, specifically for events send
    outside of Celery."""

    task_result_queue_name: str
    """Name of the queue on RabbitMQ on the Celery side, used for results from tasks."""
    task_progress_queue_name: str
    """Name of the queue on RabbitMQ on the Celery side, used for events from tasks."""
    log_level: str
    """Log level for orchestrator."""

    def __init__(self) -> None:
        """Construct the orchestrator configuration using environment variables."""
        self.celery_config = CeleryConfig()
        self.rabbitmq_omotes = EnvRabbitMQConfig("SDK_")
        self.rabbitmq_worker_events = EnvRabbitMQConfig("TASK_")

        self.task_result_queue_name = os.environ.get(
            "TASK_RESULT_QUEUE_NAME", "omotes_task_result_events"
        )
        self.task_progress_queue_name = os.environ.get(
            "TASK_PROGRESS_QUEUE_NAME", "omotes_task_progress_events"
        )
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
