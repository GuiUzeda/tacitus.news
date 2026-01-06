
from pydantic import  PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    pg_dsn: PostgresDsn = "postgresql://postgre:postgre@postgre:5432/postgre"
    domains: set[str] = ["localhost"]