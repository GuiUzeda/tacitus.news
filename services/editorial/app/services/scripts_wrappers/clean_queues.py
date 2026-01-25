from app.config import Settings
from app.services.maintenance import MaintenanceService
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def clean_queues_wrapper():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        service = MaintenanceService(session)
        print(service.clear_completed_queues())


if __name__ == "__main__":
    clean_queues_wrapper()
