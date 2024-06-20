import os
from dataclasses import dataclass
from typing import Optional

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


class PostgreSQLConfig:
    """Retrieve PostgreSQL configuration from environment variables."""

    host: str
    port: int
    database: str
    username: Optional[str]
    password: Optional[str]

    def __init__(self, prefix: str = ""):
        """Create the PostgreSQL configuration and retrieve values from env vars.

        :param prefix: Prefix to the name environment variables.
        """
        self.host = os.environ.get(f"{prefix}POSTGRESQL_HOST", "localhost")
        self.port = int(os.environ.get(f"{prefix}POSTGRESQL_PORT", "5432"))
        self.database = os.environ.get(f"{prefix}POSTGRESQL_DATABASE", "public")
        self.username = os.environ.get(f"{prefix}POSTGRESQL_USERNAME")
        self.password = os.environ.get(f"{prefix}POSTGRESQL_PASSWORD")


class PostgresJobManagerConfig:
    """Retrieve PostgresJobManager configuration from environment variables."""

    job_retention_sec: int
    """The allowed retention time in seconds of a database job row"""

    def __init__(self, prefix: str = ""):
        """Create the PostgresJobManager configuration and retrieve values from env vars.

        :param prefix: Prefix to the name environment variables.
        """
        """Default database job row retention duration to be 48 hours."""
        self.job_retention_sec = int(os.environ.get(f"{prefix}JOB_RETENTION_SEC", "172800"))


@dataclass
class OrchestratorConfig:
    """Configuration class for orchestrator."""

    celery_config: CeleryConfig
    """Configuration for Celery app."""
    postgres_config: PostgreSQLConfig
    """Configuration for PostgreSQL database for job persistence."""
    postgres_job_manager_config: PostgresJobManagerConfig
    """Configuration for PostgresJobManager component."""
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
        self.postgres_config = PostgreSQLConfig()
        self.postgres_job_manager_config = PostgresJobManagerConfig()
        self.rabbitmq_omotes = EnvRabbitMQConfig("SDK_")
        self.rabbitmq_worker_events = EnvRabbitMQConfig("TASK_")

        self.task_result_queue_name = os.environ.get(
            "TASK_RESULT_QUEUE_NAME", "omotes_task_result_events"
        )
        self.task_progress_queue_name = os.environ.get(
            "TASK_PROGRESS_QUEUE_NAME", "omotes_task_progress_events"
        )
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
