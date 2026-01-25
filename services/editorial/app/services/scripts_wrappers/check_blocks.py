from app.config import Settings
from app.services.health_service import HealthService
from rich.console import Console
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def check_blocks_wrapper():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)
    console = Console()

    with SessionLocal() as session:
        service = HealthService(session)
        result = service.check_blocks()
        console.print(result)


if __name__ == "__main__":
    check_blocks_wrapper()
