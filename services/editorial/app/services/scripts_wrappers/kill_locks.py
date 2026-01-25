from app.config import Settings
from app.services.maintenance import MaintenanceService
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def kill_locks_wrapper():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        service = MaintenanceService(session)
        print(service.emergency_unlock_db())


if __name__ == "__main__":
    kill_locks_wrapper()
