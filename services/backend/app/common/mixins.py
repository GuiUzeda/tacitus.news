from sqlalchemy.orm import Session


class DBSessionMixin:
    """Provides instance of database session."""

    def __init__(self, db_session: Session, **kwargs) -> None:
        super().__init__(**kwargs)
        self.db_session = db_session
