import threading
import uuid
from dataclasses import dataclass
from functools import partial
from typing import List, Any

from celery import Celery, Task as CeleryTask
from celery.events import EventReceiver
from celery.result import AsyncResult

from omotes_sdk.config import RabbitMQConfig
from omotes_sdk.workflow_type import WorkflowType


@dataclass
class PostgreSQLConfig:
    host: str
    port: int
    database: str
    username: str
    password: str


class CeleryInterface:
    app: Celery
    rabbitmq_config: RabbitMQConfig
    postgresql_config: PostgreSQLConfig

    def __init__(self, rabbitmq_config: RabbitMQConfig, postgresql_config: PostgreSQLConfig) -> None:
        self.rabbitmq_config = rabbitmq_config
        self.postgresql_config = postgresql_config

    def start(self) -> None:
        postgresql_config = self.postgresql_config
        rabbitmq_config = self.rabbitmq_config

        #  TODO set late ack, prefetch == 1, ignore results (will the job still exist in postgresql? How does queueing work in this case?),
        self.app = Celery(
            "omotes",
            backend=f"db+postgresql://{postgresql_config.username}:{postgresql_config.password}@{postgresql_config.host}:{postgresql_config.port}/{postgresql_config.database}",
            broker=f"pyamqp://{rabbitmq_config.username}:{rabbitmq_config.password}@{rabbitmq_config.host}:{rabbitmq_config.port}/{rabbitmq_config.virtual_host}",
        )

    def stop(self) -> None:
        self.app.close()

    def start_workflow(self, workflow_type: WorkflowType, job_id: uuid.UUID, input_esdl: bytes) -> None:
        self.app.signature(
            workflow_type.workflow_type_name, (job_id, input_esdl), queue=workflow_type.workflow_type_name
        ).delay()

    def retrieve_result(self, celery_task_id: str) -> Any:
        return self.app.AsyncResult(celery_task_id).get()
