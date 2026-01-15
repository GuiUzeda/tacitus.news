"""enrich queue

Revision ID: 5378e688d1e8
Revises: e41c73e0b705
Create Date: 2026-01-13 07:25:47.625500

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5378e688d1e8'
down_revision: Union[str, Sequence[str], None] = 'e41c73e0b705'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add 'ENRICH' to the articlesqueuename enum
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE articlesqueuename ADD VALUE IF NOT EXISTS 'ENRICH'")


def downgrade() -> None:
    """Downgrade schema."""
    # PostgreSQL does not support removing values from an ENUM type easily.
    # We leave it as is for downgrade.
    pass
