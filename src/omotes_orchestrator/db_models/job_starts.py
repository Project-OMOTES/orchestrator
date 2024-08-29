import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import sqlalchemy as db
from sqlalchemy import ForeignKey

from omotes_orchestrator.db_models.base import Base


@dataclass
class JobStartsDB(Base):
    """SQL table definition for a registered job.

    Note: Entries are removed from the table when they are finished (regardless of result type).
    """

    __tablename__ = "job_starts"

    job_id: uuid.UUID = db.Column(ForeignKey("job.job_id"), primary_key=True)
    """OMOTES identifier for the job."""
    started_at: Optional[datetime] = db.Column(db.DateTime(timezone=True), primary_key=True)
    """Time at which a Celery worker has started the task for this job."""
