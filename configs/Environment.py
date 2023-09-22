import os
from typing import Optional
from functools import lru_cache

class AppSetting():
    def __init__(self):
        self.CELERY_BROKER_URL:str = os.environ['CELERY_BROKER_URL']
        self.ACCELERATOR_API_URL: str =os.environ['CELERY_BROKER_URL']

@lru_cache
def get_environment_variables():
    return AppSetting()