import os
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from omotes_sdk.job_status import JobStatus
from omotes_sdk.prefect_util import MinioResource, TimeseriesResource

# Set required env vars for testing before importing orchestrator modules
os.environ.setdefault("PREFECT_API_URL", "http://localhost:4200/api")
os.environ.setdefault("PREFECT_API_AUTH_STRING", "test-token")
os.environ.setdefault("MINIO_HOST", "localhost")
os.environ.setdefault("MINIO_PORT", "9000")
os.environ.setdefault("MINIO_ACCESS_KEY", "test-access-key")
os.environ.setdefault("MINIO_SECRET", "test-secret")

from orchestrator.database import Job, JobCleanupResource  # noqa: E402
from orchestrator.main import create_app  # noqa: E402
from orchestrator.routes import job as job_routes  # noqa: E402


class InMemoryJobStore:
    """Minimal durable-store substitute for route unit tests."""

    def __init__(self) -> None:
        """Initialize isolated job and resource collections."""
        self.jobs: dict[UUID, Job] = {}
        self.resources: dict[UUID, dict[str, JobCleanupResource]] = {}

    async def create_job(
        self,
        *,
        job_id: UUID,
        job_name: str,
        workflow_type: str,
        workflow_version: str | None,
        user_name: str,
        status: JobStatus,
    ) -> None:
        """Store a job in memory."""
        self.jobs[job_id] = Job(
            job_id=job_id,
            job_name=job_name,
            workflow_type=workflow_type,
            workflow_version=workflow_version,
            user_name=user_name,
            status=status,
        )

    async def get_job(self, job_id: UUID) -> Job | None:
        """Return a stored job."""
        return self.jobs.get(job_id)

    async def list_jobs(self) -> list[Job]:
        """Return non-deleted jobs."""
        return [job for job in self.jobs.values() if job.deleted_at is None]

    async def update_job_status(self, job_id: UUID, status: JobStatus) -> None:
        """Update a stored status."""
        if job_id in self.jobs:
            self.jobs[job_id] = replace(self.jobs[job_id], status=status)

    async def mark_job_deleted(self, job_id: UUID) -> None:
        """Mark a stored job deleted."""
        if job_id in self.jobs:
            self.jobs[job_id] = replace(self.jobs[job_id], deleted_at=datetime.now(UTC))

    async def register_resources(self, job_id: UUID, resources: list[MinioResource | TimeseriesResource]) -> None:
        """Register resources using their serialized payload as a stable key."""
        stored = self.resources.setdefault(job_id, {})
        for resource in resources:
            resource_key = resource.model_dump_json(by_alias=True)
            stored[resource_key] = JobCleanupResource(resource_key=resource_key, resource=resource)

    async def get_cleanup_resources(self, job_id: UUID) -> list[JobCleanupResource]:
        """Return resources still awaiting cleanup."""
        return list(self.resources.get(job_id, {}).values())

    async def record_cleanup_result(self, job_id: UUID, resource_key: str, error: str | None) -> None:
        """Remove successfully cleaned resources from the active set."""
        if error is None:
            self.resources.get(job_id, {}).pop(resource_key, None)


@pytest.fixture
def job_store() -> InMemoryJobStore:
    """Provide an isolated job store."""
    return InMemoryJobStore()


@pytest.fixture
def client(job_store: InMemoryJobStore) -> Iterator[TestClient]:
    """Create an API client backed by the isolated job store."""
    app = create_app()
    app.dependency_overrides[job_routes.get_job_store] = lambda: job_store
    with TestClient(app) as test_client:
        yield test_client
