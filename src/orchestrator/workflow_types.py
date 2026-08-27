"""Dataclasses for in-memory workflow state."""

from dataclasses import dataclass, field

from omotes_sdk import MemoryLimit

from orchestrator.jsonforms import JsonFormsProperties, validate_jsonforms_properties


@dataclass(slots=True)
class WorkflowDefinition:
    """In-memory workflow definition."""

    workflow_type_name: str
    workflow_type_description_name: str
    prefect_flow_name: str
    workflow_parameters: JsonFormsProperties = field(default_factory=dict)
    memory_limit: MemoryLimit | None = None

    def __post_init__(self) -> None:
        """Validate workflow parameters and normalize the memory limit."""
        self.workflow_parameters = validate_jsonforms_properties(dict(self.workflow_parameters))
        if isinstance(self.memory_limit, str):
            self.memory_limit = MemoryLimit(self.memory_limit)
