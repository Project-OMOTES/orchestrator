"""Pydantic models for the Omotes REST API."""

from typing import Any
from uuid import UUID

from omotes_sdk.job_status import JobStatus
from pydantic import BaseModel, Field, field_validator

from orchestrator.jsonforms import JsonFormsProperties, JsonSchemaObject, validate_jsonforms_properties


class WorkflowResponse(BaseModel):
    """Response with available workflows."""

    id: str
    description: str
    versions: list[str] = Field(default_factory=list)
    schema: JsonSchemaObject | None = None  # noqa: A003 - required by downstream services
    uischema: JsonSchemaObject | None = None


class WorkflowUpload(BaseModel):
    """Workflow definition uploaded through the API."""

    workflow_type_name: str
    workflow_type_description_name: str
    prefect_flow_name: str
    memory_limit: str | None = None
    prefect_flow_version: str | None = None
    versions: list[str] = Field(default_factory=list)
    workflow_parameters: JsonFormsProperties = Field(default_factory=dict)

    @field_validator("workflow_parameters")
    @classmethod
    def _validate_workflow_parameters(cls, value: JsonFormsProperties) -> JsonFormsProperties:
        return validate_jsonforms_properties(value)


class JobInput(BaseModel):
    """Input needed to start a new job."""

    job_name: str = Field(default="job name")
    workflow_type: str = Field(default="grow_optimizer_no_heat_losses")
    version: str | None = Field(default=None)
    user_name: str = Field(default="user name")
    input_esdl: str = Field(default="input ESDL base64string")
    input_params_dict: dict[str, Any] = Field(default_factory=dict)


class JobStatusResponse(BaseModel):
    """Response with job status."""

    job_id: UUID
    status: JobStatus


class JobDeleteResponse(BaseModel):
    """Response for job deletion."""

    job_id: UUID
    deleted: bool


class JobSummary(BaseModel):
    """Summary of a job."""

    job_id: UUID
    job_name: str
    status: JobStatus
    user_name: str
    project_name: str


class JobResponse(BaseModel):
    """Full job response with all details."""

    job_id: UUID
    job_name: str
    status: JobStatus
    user_name: str
    workflow_type: str
    progress_fraction: float | None = None
    progress_message: str | None = None
    input_esdl: str | None = None
    output_esdl: str | None = None
    input_params_dict: dict[str, Any] = Field(default_factory=dict)
    timeout_after_s: int
    logs: str
    esdl_feedback: list[dict]
    job_priority: str | None = None
