from app.config import Settings
from app.services.backup_service import BackupService
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def restore_wrapper():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        service = BackupService(session)
        print(service.restore_all())


if __name__ == "__main__":
    restore_wrapper()
