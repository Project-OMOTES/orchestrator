import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()


class JobStatus(Enum):
    SUBMITTED = "submitted"
    RUNNING = "running"


@dataclass
class JobDB(Base):
    __tablename__ = "job"

    job_id: uuid.UUID = db.Column(UUID(as_uuid=True), primary_key=True)
    celery_id: str = db.Column(db.String)
    status: JobStatus = db.Column(db.Enum(JobStatus), nullable=False)
    registered_at: datetime = db.Column(db.DateTime(timezone=True), nullable=False)
    running_at: datetime = db.Column(db.DateTime(timezone=True))
    timeout_after_ms: int = db.Column(db.Integer)
    is_cancelled: bool = db.Column(db.Boolean, nullable=False, default=False)
