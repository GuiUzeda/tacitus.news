"""add rate limit tables

Revision ID: 20260123_add_rate_limit_tables
Revises: 9daae42bb19a_fix_typo
Create Date: 2026-01-23 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '20260123_add_rate_limit'
down_revision = '9daae42bb19a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create Enum Type
    rate_limit_metric = postgresql.ENUM('TOKENS', 'REQUESTS', name='ratelimitmetric')
    rate_limit_metric.create(op.get_bind(), checkfirst=True)

    # Create Config Table
    op.create_table(
        'rate_limit_config',
        sa.Column('model_id', sa.String(), nullable=False),
        sa.Column('metric', sa.Enum('TOKENS', 'REQUESTS', name='ratelimitmetric'), nullable=False),
        sa.Column('window_seconds', sa.Integer(), nullable=False),
        sa.Column('limit_value', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('model_id', 'metric')
    )

    # Create Usage Table
    op.create_table(
        'rate_limit_usage',
        sa.Column('key', sa.String(), nullable=False),
        sa.Column('current_value', sa.Integer(), nullable=False),
        sa.Column('window_start', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('key')
    )


def downgrade() -> None:
    op.drop_table('rate_limit_usage')
    op.drop_table('rate_limit_config')
    sa.Enum(name='ratelimitmetric').drop(op.get_bind(), checkfirst=False)
