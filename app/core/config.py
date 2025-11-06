# app/core/config.py
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    BSALE_API_TOKEN: str
    # BigQuery es obligatorio ahora
    BIGQUERY_PROJECT: str
    BIGQUERY_DATASET: str

    class Config:
        env_file = ".env"


settings = Settings()