import uuid
from dataclasses import dataclass
from typing import Any

from celery import Celery

from omotes_sdk.workflow_type import WorkflowType
from omotes_sdk.internal.common.config import RabbitMQConfig, PostgreSQLConfig

from omotes_orchestrator.config import CeleryConfig


class CeleryInterface:
    app: Celery
    config: CeleryConfig

    def __init__(self, config: CeleryConfig) -> None:
        self.config = config

    def start(self) -> None:
        postgresql_config = self.config.postgresql_config
        rabbitmq_config = self.config.rabbitmq_config

        self.app = Celery(
            backend=f"db+postgresql://{postgresql_config.username}:{postgresql_config.password}@"
            f"{postgresql_config.host}:{postgresql_config.port}/"
            f"{postgresql_config.database}",
            broker=f"pyamqp://{rabbitmq_config.username}:{rabbitmq_config.password}@"
            f"{rabbitmq_config.host}:{rabbitmq_config.port}/{rabbitmq_config.virtual_host}",
        )

    def stop(self) -> None:
        self.app.close()

    def start_workflow(
        self, workflow_type: WorkflowType, job_id: uuid.UUID, input_esdl: bytes
    ) -> None:
        self.app.signature(
            workflow_type.workflow_type_name,
            (job_id, input_esdl),
            queue=workflow_type.workflow_type_name,
        ).delay()

    def retrieve_result(self, celery_task_id: str) -> Any:
        return self.app.AsyncResult(celery_task_id).get()
