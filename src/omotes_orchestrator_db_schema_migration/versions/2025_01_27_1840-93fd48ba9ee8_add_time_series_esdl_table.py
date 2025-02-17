"""add time series esdl table

Revision ID: 93fd48ba9ee8
Revises: 60506a3a582f
Create Date: 2025-01-27 18:40:46.490777

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "93fd48ba9ee8"
down_revision: Union[str, None] = "60506a3a582f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "esdl_time_series_info",
        sa.Column("row_id", sa.UUID(), nullable=False),
        sa.Column("esdl_id", sa.String(), nullable=True),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("job_id", sa.UUID(), nullable=True),
        sa.Column("job_reference", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("row_id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("esdl_time_series_info")
    # ### end Alembic commands ###
