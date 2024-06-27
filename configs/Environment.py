import os
from typing import Optional, get_type_hints
from functools import lru_cache

class AppSetting():
    def __init__(self):
        self.CELERY_BROKER_URL:str = os.environ['CELERY_BROKER_URL']
        self.ACCELERATOR_CLI_BASE_URL: str = os.environ['ACCELERATOR_CLI_BASE_URL']
        self.IMAGE_REGISTRY_URL: str = os.environ.get('IMAGE_REGISTRY_URL')
        self.IMAGE_REGISTRY_USER: str = os.environ.get('IMAGE_REGISTRY_USER')
        self.IMAGE_REGISTRY_PASSWORD: str = os.environ.get('IMAGE_REGISTRY_PASSWORD')
        self.WKUBE_SECRET_JSON_B64: str = os.environ.get('WKUBE_SECRET_JSON_B64')
        self.WKUBE_K8_NAMESPACE: str = os.environ.get('WKUBE_K8_NAMESPACE', "wkube")
        self.WKUBE_AUTO_GITHUB_PAT: str = os.environ.get('WKUBE_AUTO_GITHUB_PAT', "")

@lru_cache
def get_environment_variables():
    settings = AppSetting()

    annotations = get_type_hints(settings.__init__)

    for key, value_type in annotations.items():
        if value_type == bool:
            value = getattr(settings, key)
            if type(value) == str:
                if value == 'True':
                    setattr(settings, key, True)
                elif value == 'False':
                    setattr(settings, key, False)
                else:
                    raise ValueError(f"Value does not match annotation of AppSetting: {key}")

    return settings