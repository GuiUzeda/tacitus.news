from pydantic import PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

from pathlib import Path


script_dir = Path(__file__).resolve().parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", env_file=script_dir / ".env")
    pg_dsn: PostgresDsn = PostgresDsn("postgresql://postgre:postgre@localhost:5432/postgre")
    feeds_path: str = "./services/editorial/data/feeds.json"
    save_hourly: bool = True
    domains: set[str] = {"localhost"}
    gemini_api_key: str = ""
    script_dir: Path = script_dir
    similarity_strict = 0.20
    similarity_loose = 0.35
    
