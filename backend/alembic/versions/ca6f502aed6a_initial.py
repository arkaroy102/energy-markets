"""initial

Revision ID: ca6f502aed6a
Revises:
Create Date: 2026-04-28 16:32:47.362330

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ca6f502aed6a'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'node_table',
        sa.Column('node_id', sa.Integer(), primary_key=True),
        sa.Column('grid', sa.Enum('ERCOT', 'NYISO', 'CAISO', name='grid_enum'), nullable=False),
        sa.Column('node_name', sa.String(), nullable=False),
        sa.Column('node_type', sa.Enum('ELECTRICAL_BUS', 'GENERATOR', name='node_type_enum'), nullable=False),
        sa.Column('external_id', sa.String(), nullable=True),
        sa.Column('substation', sa.String(), nullable=True),
        sa.Column('voltage_level', sa.Float(), nullable=True),
        sa.Column('settlement_load_zone', sa.String(), nullable=True),
        sa.Column('latitude', sa.Float(), nullable=True),
        sa.Column('longitude', sa.Float(), nullable=True),
        sa.UniqueConstraint('grid', 'node_name', name='uq_node'),
    )
    op.create_table(
        'node_price_table',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('node_id', sa.Integer(), sa.ForeignKey('node_table.node_id'), nullable=False),
        sa.Column('timestamp_utc', sa.DateTime(timezone=True), nullable=False),
        sa.Column('lmp', sa.Float(), nullable=False),
        sa.UniqueConstraint('node_id', 'timestamp_utc', name='uq_price'),
    )


def downgrade() -> None:
    op.drop_table('node_price_table')
    op.drop_table('node_table')
    op.execute('DROP TYPE IF EXISTS grid_enum')
    op.execute('DROP TYPE IF EXISTS node_type_enum')
