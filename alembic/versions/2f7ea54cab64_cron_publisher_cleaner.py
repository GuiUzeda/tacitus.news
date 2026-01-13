"""cron publisher cleaner

Revision ID: 2f7ea54cab64
Revises: cf7b24cacc50
Create Date: 2026-01-13 13:54:12.191270

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2f7ea54cab64'
down_revision: Union[str, Sequence[str], None] = 'cf7b24cacc50'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # 1. Enable Extension (Must be superuser usually)
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")

    # 2. Create the Cleanup Function
    # Logic: Archive Published events that are:
    #   a) > 7 days old AND score < 50
    #   b) > 14 days old (regardless of score)
    op.execute("""
    CREATE OR REPLACE FUNCTION archive_stale_events_proc() RETURNS void AS $$
    BEGIN
        -- Log start (Optional, goes to Postgres logs)
        RAISE NOTICE 'Running Archivist Cleanup...';

        -- Policy A: Old & Low Score
        UPDATE news_events
        SET status = 'ARCHIVED', is_active = false
        WHERE status = 'PUBLISHED'
          AND last_updated_at < NOW() - INTERVAL '7 days'
          AND hot_score < 50.0;

        -- Policy B: Ancient History
        UPDATE news_events
        SET status = 'ARCHIVED', is_active = false
        WHERE status = 'PUBLISHED'
          AND last_updated_at < NOW() - INTERVAL '14 days';
          
    END;
    $$ LANGUAGE plpgsql;
    """)

    # 3. Schedule the Cron Job (Runs daily at 03:00 UTC)
    # The job name 'archivist_daily_sweep' ensures uniqueness
    op.execute("""
    SELECT cron.schedule(
        'archivist_daily_sweep', 
        '0 3 * * *', 
        'SELECT archive_stale_events_proc()'
    );
    """)


def downgrade() -> None:
    # 1. Unschedule
    op.execute("SELECT cron.unschedule('archivist_daily_sweep');")

    # 2. Drop Function
    op.execute("DROP FUNCTION IF EXISTS archive_stale_events_proc;")

    # 3. Drop Extension (Optional, usually safer to leave enabled)
    # op.execute("DROP EXTENSION IF EXISTS pg_cron;")
