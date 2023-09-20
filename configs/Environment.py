from typing import Optional
from functools import lru_cache
from pydantic_settings import BaseSettings

class AppSetting(BaseSettings):
    CELERY_BROKER_URL:str
    ACCELERATOR_API_URL: str

@lru_cache
def get_environment_variables():
    return AppSetting()