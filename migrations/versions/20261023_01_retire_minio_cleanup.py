"""Retire cleanup records for the discarded MinIO object store.

Revision ID: 20261023_01
Revises: 20260928_01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20261023_01"
down_revision: str | None = "20260928_01"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Mark obsolete MinIO cleanup resources as deleted without removing jobs."""
    op.execute(
        sa.text(
            "UPDATE job_cleanup_resources "
            "SET cleanup_status = 'DELETED', cleaned_at = now(), updated_at = now() "
            "WHERE resource_type = 'minio' AND cleanup_status IN ('ACTIVE', 'FAILED')"
        )
    )


def downgrade() -> None:
    """Do not restore cleanup records for data that has been discarded."""
