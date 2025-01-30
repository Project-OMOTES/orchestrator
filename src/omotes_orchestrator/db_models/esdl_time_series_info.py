import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import sqlalchemy as db
from sqlalchemy.dialects.postgresql import UUID

from omotes_orchestrator.db_models.base import Base


@dataclass
class EsdlTimeSeriesInfoDB(Base):
    """SQL table definition for a time series data management."""

    __tablename__ = "esdl_time_series_info"

    row_id: uuid.UUID = db.Column(UUID(as_uuid=True), primary_key=True)  # type: ignore [misc]
    """ESDL time series info identifier, needed as primary key since other keys are nullable."""
    esdl_id: str = db.Column(db.String, nullable=True)
    """Output ESDL id which this time series belongs to."""
    registered_at: datetime = db.Column(db.DateTime(timezone=True))
    """Time at which this data entry was registered, should be on job completion."""
    deactivated_at: Optional[datetime] = db.Column(db.DateTime(timezone=True), nullable=True)
    """Time at which the time series data was set to inactive, upon deletion/job cancellation."""
    job_id: Optional[uuid.UUID]= db.Column(UUID(as_uuid=True), nullable=True)  # type: ignore [misc]
    """Optional omotes job id."""
    job_reference: Optional[str] = db.Column(db.String, nullable=True)
    """Optional omotes job reference."""
    size_mb: Optional[float] = db.Column(db.Float, nullable=True)
    """Optional approximate database size in MB."""
