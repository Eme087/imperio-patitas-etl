# app/core/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str
    BSALE_API_TOKEN: str

    class Config:
        env_file = ".env"

settings = Settings()