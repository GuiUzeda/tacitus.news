import sys
import os
from sqlalchemy import create_engine, text
from loguru import logger

# Add service root to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Settings

def kill_locks():
    """
    Terminates all other database connections to release locks for migrations.
    """
    settings = Settings()
    # isolation_level="AUTOCOMMIT" is often required to run termination commands
    engine = create_engine(str(settings.pg_dsn), isolation_level="AUTOCOMMIT")
    
    logger.warning("🔪 Killing active database connections to release locks...")
    
    with engine.connect() as conn:
        stmt = text("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = current_database()
            AND pid <> pg_backend_pid();
        """)
        conn.execute(stmt)
        logger.success("✅ Connections terminated. You can now run migrations.")

if __name__ == "__main__":
    kill_locks()