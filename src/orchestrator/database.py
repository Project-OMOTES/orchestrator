"""Durable storage for jobs and their cleanup resources."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from omotes_sdk.job_status import JobStatus
from omotes_sdk.prefect_util import MinioResource, TimeseriesResource
from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, Text, select, update
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from orchestrator.settings import settings


class Base(DeclarativeBase):
    """Base class for orchestrator database tables."""


class CleanupStatus(StrEnum):
    """Lifecycle status of a registered cleanup resource."""

    ACTIVE = "ACTIVE"
    FAILED = "FAILED"
    DELETED = "DELETED"


class JobRow(Base):
    """Persistent job identity, metadata, and last known status."""

    __tablename__ = "jobs"

    job_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    job_name: Mapped[str] = mapped_column(Text)
    workflow_type: Mapped[str] = mapped_column(Text)
    workflow_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_name: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class JobCleanupResourceRow(Base):
    """A durable, retryable cleanup declaration owned by a job."""

    __tablename__ = "job_cleanup_resources"

    job_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), ForeignKey("jobs.job_id"), index=True)
    resource_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    resource_type: Mapped[str] = mapped_column(String(32))
    resource_data: Mapped[dict[str, Any]] = mapped_column(JSONB)
    cleanup_status: Mapped[CleanupStatus] = mapped_column(
        Enum(
            CleanupStatus,
            name="ck_job_cleanup_resources_cleanup_status",
            native_enum=False,
            create_constraint=True,
            length=32,
        ),
        default=CleanupStatus.ACTIVE,
        index=True,
    )
    cleanup_attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_cleanup_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )
    cleaned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


@dataclass(frozen=True)
class Job:
    """Job data returned by the durable store."""

    job_id: UUID
    job_name: str
    workflow_type: str
    workflow_version: str | None
    user_name: str
    status: JobStatus
    deleted_at: datetime | None = None


@dataclass(frozen=True)
class JobCleanupResource:
    """Cleanup resource with its stable database key."""

    resource_key: str
    resource: MinioResource | TimeseriesResource


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _engine, _session_factory
    if _session_factory is None:
        url = URL.create(
            "postgresql+psycopg",
            username=settings.orchestrator_database_username,
            password=settings.orchestrator_database_password,
            host=settings.orchestrator_database_host,
            port=settings.orchestrator_database_port,
            database=settings.orchestrator_database_name,
        )
        _engine = create_async_engine(url, pool_pre_ping=True)
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _session_factory


async def dispose_database() -> None:
    """Dispose pooled database connections during application shutdown."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


def _to_stored_job(row: JobRow) -> Job:
    return Job(
        job_id=row.job_id,
        job_name=row.job_name,
        workflow_type=row.workflow_type,
        workflow_version=row.workflow_version,
        user_name=row.user_name,
        status=JobStatus(row.status),
        deleted_at=row.deleted_at,
    )


def _resource_payload(resource: MinioResource | TimeseriesResource) -> dict[str, Any]:
    return resource.model_dump(mode="json", by_alias=True)


def _resource_key(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class JobStore:
    """Read and update durable job state using short-lived sessions."""

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
        """Persist a newly submitted job."""
        async with _get_session_factory()() as session:
            session.add(
                JobRow(
                    job_id=job_id,
                    job_name=job_name,
                    workflow_type=workflow_type,
                    workflow_version=workflow_version,
                    user_name=user_name,
                    status=status.value,
                )
            )
            await session.commit()

    async def get_job(self, job_id: UUID) -> Job | None:
        """Return a job by ID, including soft-deleted jobs."""
        async with _get_session_factory()() as session:
            row = await session.get(JobRow, job_id)
            return _to_stored_job(row) if row is not None else None

    async def list_jobs(self) -> list[Job]:
        """Return all jobs that have not been soft-deleted."""
        async with _get_session_factory()() as session:
            result = await session.scalars(
                select(JobRow).where(JobRow.deleted_at.is_(None)).order_by(JobRow.created_at.desc())
            )
            return [_to_stored_job(row) for row in result]

    async def update_job_status(self, job_id: UUID, status: JobStatus) -> None:
        """Store the latest status observed from Prefect."""
        async with _get_session_factory()() as session:
            await session.execute(
                update(JobRow).where(JobRow.job_id == job_id).values(status=status.value, updated_at=datetime.now(UTC))
            )
            await session.commit()

    async def mark_job_deleted(self, job_id: UUID) -> None:
        """Soft-delete a job while retaining its audit record."""
        async with _get_session_factory()() as session:
            await session.execute(
                update(JobRow)
                .where(JobRow.job_id == job_id)
                .values(deleted_at=datetime.now(UTC), updated_at=datetime.now(UTC))
            )
            await session.commit()

    async def register_cleanup_resources(
        self, job_id: UUID, resources: list[MinioResource | TimeseriesResource]
    ) -> None:
        """Idempotently register resources owned by a job."""
        if not resources:
            return
        now = datetime.now(UTC)
        async with _get_session_factory()() as session:
            for resource in resources:
                payload = _resource_payload(resource)
                statement = insert(JobCleanupResourceRow).values(
                    job_id=job_id,
                    resource_key=_resource_key(payload),
                    resource_type=resource.type,
                    resource_data=payload,
                    cleanup_status=CleanupStatus.ACTIVE,
                    cleanup_attempts=0,
                    created_at=now,
                    updated_at=now,
                )
                statement = statement.on_conflict_do_update(
                    index_elements=["resource_key"],
                    set_={
                        "resource_data": statement.excluded.resource_data,
                        "resource_type": statement.excluded.resource_type,
                        "cleanup_status": CleanupStatus.ACTIVE,
                        "last_cleanup_error": None,
                        "cleaned_at": None,
                        "updated_at": now,
                    },
                )
                await session.execute(statement)
            await session.commit()

    async def get_cleanup_resources(self, job_id: UUID) -> list[JobCleanupResource]:
        """Return active and previously failed resources for cleanup."""
        async with _get_session_factory()() as session:
            rows = await session.scalars(
                select(JobCleanupResourceRow).where(
                    JobCleanupResourceRow.job_id == job_id,
                    JobCleanupResourceRow.cleanup_status.in_((CleanupStatus.ACTIVE, CleanupStatus.FAILED)),
                )
            )
            resources: list[JobCleanupResource] = []
            for row in rows:
                resource_type = row.resource_data.get("type")
                model = MinioResource if resource_type == "minio" else TimeseriesResource
                resources.append(
                    JobCleanupResource(resource_key=row.resource_key, resource=model.model_validate(row.resource_data))
                )
            return resources

    async def record_cleanup_result(self, job_id: UUID, resource_key: str, error: str | None) -> None:
        """Record one cleanup attempt and whether it succeeded."""
        now = datetime.now(UTC)
        values: dict[str, Any] = {
            "cleanup_status": CleanupStatus.FAILED if error else CleanupStatus.DELETED,
            "cleanup_attempts": JobCleanupResourceRow.cleanup_attempts + 1,
            "last_cleanup_error": error,
            "updated_at": now,
            "cleaned_at": None if error else now,
        }
        async with _get_session_factory()() as session:
            await session.execute(
                update(JobCleanupResourceRow)
                .where(JobCleanupResourceRow.job_id == job_id, JobCleanupResourceRow.resource_key == resource_key)
                .values(**values)
            )
            await session.commit()


job_store = JobStore()
