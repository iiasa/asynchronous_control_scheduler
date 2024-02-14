import os
from typing import Optional
from functools import lru_cache

class AppSetting():
    def __init__(self):
        self.CELERY_BROKER_URL:str = os.environ['CELERY_BROKER_URL']
        self.ACCELERATOR_CLI_BASE_URL: str = os.environ['ACCELERATOR_CLI_BASE_URL']
        self.IMAGE_REGISTRY_URL: str = os.environ.get('IMAGE_REGISTRY_URL')
        self.IMAGE_REGISTRY_USER: str = os.environ.get('IMAGE_REGISTRY_USER')
        self.IMAGE_REGISTRY_PASSWORD: str = os.environ.get('IMAGE_REGISTRY_PASSWORD')

@lru_cache
def get_environment_variables():
    return AppSetting()