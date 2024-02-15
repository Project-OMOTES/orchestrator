import os
from dataclasses import dataclass

from omotes_sdk.internal.common.config import (
    RabbitMQConfig,
    EnvRabbitMQConfig,
)


@dataclass
class CeleryConfig:
    rabbitmq_config: RabbitMQConfig

    def __init__(self):
        self.rabbitmq_config = EnvRabbitMQConfig("CELERY_")


@dataclass
class OrchestratorConfig:
    celery_config: CeleryConfig
    rabbitmq_omotes: RabbitMQConfig
    rabbitmq_worker_events: RabbitMQConfig

    task_result_queue_name: str
    task_progress_queue_name: str
    log_level: str

    def __init__(self):
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
