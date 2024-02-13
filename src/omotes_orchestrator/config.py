import os
from dataclasses import dataclass

from omotes_sdk.internal.common.config import (
    RabbitMQConfig,
    PostgreSQLConfig,
    EnvPostgreSQLConfig,
    EnvRabbitMQConfig,
)


@dataclass
class CeleryConfig:
    postgresql_config: PostgreSQLConfig
    rabbitmq_config: RabbitMQConfig

    def __init__(self):
        self.postgresql_config = EnvPostgreSQLConfig("CELERY_")
        self.rabbitmq_config = EnvRabbitMQConfig("CELERY_")


@dataclass
class OrchestratorConfig:
    celery_config: CeleryConfig
    rabbitmq_omotes: RabbitMQConfig
    rabbitmq_worker_events: RabbitMQConfig

    task_event_queue_name: str
    log_level: str

    def __init__(self):
        self.celery_config = CeleryConfig()
        self.rabbitmq_omotes = EnvRabbitMQConfig("SDK_")
        self.rabbitmq_worker_events = EnvRabbitMQConfig("TASK_")

        self.task_event_queue_name = os.environ.get("TASK_EVENT_QUEUE_NAME", "omotes_task_events")
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
