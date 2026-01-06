from ast import Module
from io import BytesIO
from os import name, wait
from typing import Any, AsyncGenerator, Iterator

from app.core.config import Settings

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

settings = Settings()
SessionFactory = sessionmaker(
    bind=create_engine(str(settings.pg_dsn)),
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

def db_session() -> Iterator[Session]:
    """Create new database session.

    Yields:
        Database session.
    """

    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
