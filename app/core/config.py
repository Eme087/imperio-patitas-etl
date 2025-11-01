# app/core/config.py
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    DATABASE_URL: str
    BSALE_API_TOKEN: str
    # Si está presente, usamos BigQuery como destino en vez de SQLAlchemy/MySQL.
    BIGQUERY_PROJECT: Optional[str] = None
    BIGQUERY_DATASET: Optional[str] = None

    class Config:
        env_file = ".env"


settings = Settings()