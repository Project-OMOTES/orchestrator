import uuid

from celery import Celery

from omotes_sdk.workflow_type import WorkflowType

from omotes_orchestrator.config import CeleryConfig


class CeleryInterface:
    app: Celery
    config: CeleryConfig

    def __init__(self, config: CeleryConfig) -> None:
        self.config = config

    def start(self) -> None:
        rabbitmq_config = self.config.rabbitmq_config

        self.app = Celery(
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
