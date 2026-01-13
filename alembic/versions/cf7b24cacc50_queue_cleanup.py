"""queue cleanup

Revision ID: cf7b24cacc50
Revises: 5378e688d1e8
Create Date: 2026-01-13 09:16:03.362647

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cf7b24cacc50'
down_revision: Union[str, Sequence[str], None] = '5378e688d1e8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create the cleanup function
    # We keep 3 days of history for debugging purposes.
    op.execute("""
    CREATE OR REPLACE FUNCTION clean_editorial_queues() RETURNS void AS $$
    BEGIN
        -- Clean Articles Queue
        DELETE FROM articles_queue 
        WHERE status IN ('COMPLETED', 'APPROVED', 'REJECTED') 
        AND updated_at < NOW() - INTERVAL '3 days';

        -- Clean Events Queue
        DELETE FROM events_queue 
        WHERE status IN ('COMPLETED', 'APPROVED', 'REJECTED') 
        AND updated_at < NOW() - INTERVAL '3 days';
    END;
    $$ LANGUAGE plpgsql;
    """)

    # 2. Schedule the job using pg_cron (if available)
    # Runs daily at 03:00 AM UTC
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
            PERFORM cron.schedule('clean_editorial_queues_job', '0 3 * * *', 'SELECT clean_editorial_queues()');
        END IF;
    END$$;
    """)


def downgrade() -> None:
    # 1. Unschedule the job
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
            PERFORM cron.unschedule('clean_editorial_queues_job');
        END IF;
    END$$;
    """)

    # 2. Drop the function
    op.execute("DROP FUNCTION IF EXISTS clean_editorial_queues();")