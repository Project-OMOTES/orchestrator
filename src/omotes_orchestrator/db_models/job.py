import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

import sqlalchemy as db
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()


class JobStatus(Enum):
    """Phases a job progresses through.

    Note: The job is removed when it is finished (regardless of the result type). Therefor
    this state is not defined in this `Enum`.
    """

    REGISTERED = "registered"
    """Job is registered but not yet submitted to Celery."""
    SUBMITTED = "submitted"
    """Job is submitted to Celery but not yet started."""
    RUNNING = "running"
    """Job is started and waiting to complete."""


@dataclass
class JobDB(Base):
    """SQL table definition for a registered job.

    Note: Entries are removed from the table when they are finished (regardless of result type).
    """

    __tablename__ = "job"

    job_id: uuid.UUID = db.Column(UUID(as_uuid=True), primary_key=True)  # type: ignore [misc]
    """OMOTES identifier for the job."""
    celery_id: Optional[str] = db.Column(db.String, nullable=True)
    """Celery identifier for the task. Only available if job is submitted to Celery."""
    workflow_type: str = db.Column(db.String)
    """Name of the workflow this job runs."""
    status: JobStatus = db.Column(db.Enum(JobStatus), nullable=False)
    """Current status of the job."""
    registered_at: datetime = db.Column(db.DateTime(timezone=True), nullable=False)
    """Time at which the job is registered."""
    submitted_at: Optional[datetime] = db.Column(db.DateTime(timezone=True))
    """Time at which the job is submitted to Celery."""
    running_at: Optional[datetime] = db.Column(db.DateTime(timezone=True))
    """Time at which a Celery worker has started the task for this job."""
    timeout_after_ms: Optional[int] = db.Column(db.Integer)
    """Duration the job may run for before being cancelled due to timing out."""
