"""convert merge_proposals status to enum

Revision ID: a0fc6794e716
Revises: 5f839588d91d
Create Date: 2026-01-12 17:57:34.033845

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a0fc6794e716'
down_revision: Union[str, Sequence[str], None] = '5f839588d91d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

ENUM_NAME = 'jobstatus'

def upgrade() -> None:
    # 1. Normalize Data: Convert existing string values to Uppercase to match Enum
    # e.g. 'pending' -> 'PENDING'
    op.execute("UPDATE merge_proposals SET status = UPPER(status)")
    
    # Handle specific legacy mappings if you had them (optional safety net)
    op.execute("UPDATE merge_proposals SET status = 'PENDING' WHERE status = 'NEEDS_HUMAN_REVIEW'")
    op.execute("UPDATE merge_proposals SET status = 'APPROVED' WHERE status = 'EXECUTED'")

    # 2. Ensure Enum Type exists and has new values
    # We use a transaction-safe block to add values to the Enum
    with op.get_context().autocommit_block():
        # Create type if it doesn't exist (e.g. if ArticleModel wasn't created yet)
        op.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{ENUM_NAME}') THEN
                    CREATE TYPE {ENUM_NAME} AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED');
                END IF;
            END$$;
        """)
        
        # Add new values introduced in this update
        # Postgres 'ALTER TYPE ADD VALUE' cannot run inside a transaction block in older versions,
        # but autocommit_block handles this for modern Alembic/PG.
        for val in ['WAITING', 'APPROVED', 'REJECTED']:
            op.execute(f"ALTER TYPE {ENUM_NAME} ADD VALUE IF NOT EXISTS '{val}'")

    # 3. Alter the Column Type
    # We use the 'USING' clause to explicitly cast the string data to the enum type
    op.execute(f"""
        ALTER TABLE merge_proposals 
        ALTER COLUMN status TYPE {ENUM_NAME} 
        USING status::{ENUM_NAME}
    """)
    
    # 4. Set the Default value
    op.alter_column('merge_proposals', 'status', server_default='PENDING')


def downgrade() -> None:
    # 1. Revert Column to String
    op.alter_column('merge_proposals', 'status', 
                    type_=sa.String(), 
                    server_default='pending')
    
    # 2. Convert data back to lowercase (optional, to match previous state)
    op.execute("UPDATE merge_proposals SET status = LOWER(status)")

    # Note: We cannot easily remove values from a Postgres Enum (requires dropping and recreating type),
    # so we leave the Enum type as is.