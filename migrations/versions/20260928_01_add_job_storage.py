"""Add durable job and cleanup resource storage.

Revision ID: 20260928_01
Revises:
Create Date: 2026-09-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260928_01"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create durable job and cleanup resource tables."""
    op.create_table(
        "jobs",
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_name", sa.Text(), nullable=False),
        sa.Column("workflow_type", sa.Text(), nullable=False),
        sa.Column("workflow_version", sa.Text(), nullable=True),
        sa.Column("user_name", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("job_id"),
    )
    op.create_index("ix_jobs_status", "jobs", ["status"], unique=False)
    op.create_table(
        "job_cleanup_resources",
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("resource_key", sa.String(length=64), nullable=False),
        sa.Column("resource_type", sa.String(length=32), nullable=False),
        sa.Column("resource_data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("cleanup_status", sa.String(length=32), server_default="ACTIVE", nullable=False),
        sa.Column("cleanup_attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_cleanup_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("cleaned_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "cleanup_status IN ('ACTIVE', 'FAILED', 'DELETED')",
            name="ck_job_cleanup_resources_cleanup_status",
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.job_id"]),
        sa.PrimaryKeyConstraint("resource_key"),
    )
    op.create_index(
        "ix_job_cleanup_resources_cleanup_status", "job_cleanup_resources", ["cleanup_status"], unique=False
    )
    op.create_index("ix_job_cleanup_resources_job_id", "job_cleanup_resources", ["job_id"], unique=False)


def downgrade() -> None:
    """Drop durable job and cleanup resource tables."""
    op.drop_index("ix_job_cleanup_resources_job_id", table_name="job_cleanup_resources")
    op.drop_index("ix_job_cleanup_resources_cleanup_status", table_name="job_cleanup_resources")
    op.drop_table("job_cleanup_resources")
    op.drop_index("ix_jobs_status", table_name="jobs")
    op.drop_table("jobs")
