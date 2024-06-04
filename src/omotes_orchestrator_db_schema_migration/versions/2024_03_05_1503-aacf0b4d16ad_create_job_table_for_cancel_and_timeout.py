"""Create Job table for cancel and timeout.

Revision ID: aacf0b4d16ad
Revises: 
Create Date: 2024-03-05 15:03:24.452717

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "aacf0b4d16ad"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

job_status = sa.Enum("REGISTERED", "SUBMITTED", "RUNNING", name="jobstatus")


def upgrade() -> None:
    op.create_table(
        "job",
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("celery_id", sa.String(), nullable=True),
        sa.Column("workflow_type", sa.String(), nullable=False),
        sa.Column("status", job_status, nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("running_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("timeout_after_ms", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("job_id"),
    )


def downgrade() -> None:
    op.drop_table("job")
    job_status.drop(op.get_bind())
