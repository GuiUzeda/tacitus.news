from datetime import timedelta
from pathlib import Path

from app.core import logger_config
from pydantic import PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

_ = logger_config

script_dir = Path(__file__).resolve().parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", env_file=script_dir / ".env")
    pg_dsn: PostgresDsn
    feeds_path: str = "./services/editorial/data/feeds.json"
    save_hourly: bool = True
    domains: set[str] = {"localhost"}
    gemini_api_key: str = ""
    groq_api_key: str = ""
    cerebras_api_key: str = ""
    openrouter_api_key: str = ""
    script_dir: Path = script_dir
    similarity_strict: float = 0.05
    similarity_loose: float = 0.18
    similarity_loose_merger: float = 0.13
    cutoff_period: timedelta = timedelta(days=7)
    worker_cool_down: int = 10
    db_pool_size: int = 5
    db_max_overflow: int = 10
