import uuid

from celery import Celery

from omotes_sdk.workflow_type import WorkflowType

from omotes_orchestrator.config import CeleryConfig


class CeleryInterface:
    """Connect to the Celery app which orchestrates the workers."""

    app: Celery
    """The Celery app through which tasks may be monitored & started."""
    config: CeleryConfig
    """Config for the Celery app."""

    def __init__(self, config: CeleryConfig) -> None:
        """Create the interface to Celery.

        :param config: Configuration for Celery.
        """
        self.config = config

    def start(self) -> None:
        """Start the Celery app."""
        rabbitmq_config = self.config.rabbitmq_config

        self.app = Celery(
            broker=f"pyamqp://{rabbitmq_config.username}:{rabbitmq_config.password}@"
                   f"{rabbitmq_config.host}:{rabbitmq_config.port}/{rabbitmq_config.virtual_host}",
        )

    def stop(self) -> None:
        """Stop the Celery app."""
        self.app.close()

    def start_workflow(
        self,
        workflow_type: WorkflowType,
        job_id: uuid.UUID,
        input_esdl: str,
        params_dict: dict = None
    ) -> None:
        """Start a new workflow.

        :param workflow_type: Type of workflow to start. Currently, this translates directly to
            a Celery task with the same name.
        :param job_id: The OMOTES ID of the job.
        :param input_esdl: The ESDL to perform the task on.
        :param params_dict: The additional, non-ESDL, job parameters.
        """
        self.app.signature(
            workflow_type.workflow_type_name,
            (job_id, input_esdl, params_dict),
            queue=workflow_type.workflow_type_name,
        ).delay()
