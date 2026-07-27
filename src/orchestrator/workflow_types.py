"""Dataclasses for in-memory workflow state."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class WorkflowType:
    """In-memory workflow definition."""

    workflow_type_name: str
    workflow_type_description_name: str
    prefect_flow_name: str
    workflow_parameters: list[dict[str, Any]] = field(default_factory=list)
