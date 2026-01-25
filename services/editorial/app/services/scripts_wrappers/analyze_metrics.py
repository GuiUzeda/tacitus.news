from app.config import Settings
from app.services.reporting_service import ReportingService
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def analyze_metrics_wrapper():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        service = ReportingService(session)
        service.analyze_metrics()


if __name__ == "__main__":
    analyze_metrics_wrapper()
