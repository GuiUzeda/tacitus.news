from app.config import Settings
from app.services.backup_service import BackupService
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def dump_wrapper():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        service = BackupService(session)
        print(service.dump_all())


if __name__ == "__main__":
    dump_wrapper()
