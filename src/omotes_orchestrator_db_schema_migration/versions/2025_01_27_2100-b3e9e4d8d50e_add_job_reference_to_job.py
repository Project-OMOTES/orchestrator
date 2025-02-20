"""add job_reference to job

Revision ID: b3e9e4d8d50e
Revises: 93fd48ba9ee8
Create Date: 2025-01-27 21:00:24.807406

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3e9e4d8d50e'
down_revision: Union[str, None] = '93fd48ba9ee8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('job', sa.Column('job_reference', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('job', 'job_reference')
    # ### end Alembic commands ###
