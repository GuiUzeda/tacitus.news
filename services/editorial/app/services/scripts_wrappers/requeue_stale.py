from app.config import Settings
from app.services.queue_service import QueueService
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def requeue_stale_wrapper():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        service = QueueService(session)
        print(service.requeue_stale_items())


if __name__ == "__main__":
    requeue_stale_wrapper()
