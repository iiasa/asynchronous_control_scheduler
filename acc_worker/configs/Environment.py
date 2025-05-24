import os
from typing import Optional, get_type_hints
from functools import lru_cache

class AppSetting():
    def __init__(self):
        self.CELERY_BROKER_URL:str = os.environ['CELERY_BROKER_URL']
        self.ACCELERATOR_CLI_BASE_URL: str = os.environ.get('ACCELERATOR_CLI_BASE_URL', 'http://default-cli-base-url/')
        self.WKUBE_AGENT_PULLER: str = os.environ.get('WKUBE_AGENT_PULLER', 'registry.iiasa.ac.at/accelerator/wkube-agent-puller:latest')
        self.IMAGE_REGISTRY_URL: str = os.environ.get('IMAGE_REGISTRY_URL')
        # This setting is to accomodate registry which supports project name. value eg: 'accelerator/' 
        self.IMAGE_REGISTRY_TAG_PREFIX: str = os.environ.get('IMAGE_REGISTRY_TAG_PREFIX', '') 
        self.IMAGE_REGISTRY_USER: str = os.environ.get('IMAGE_REGISTRY_USER')
        self.IMAGE_REGISTRY_PASSWORD: str = os.environ.get('IMAGE_REGISTRY_PASSWORD')
        self.WKUBE_SECRET_JSON_B64: Optional[str] = os.environ.get('WKUBE_SECRET_JSON_B64', None)
        self.WKUBE_K8_NAMESPACE: str = os.environ.get('WKUBE_K8_NAMESPACE', "wkube")
        self.WKUBE_STORAGE_CLASS: str = os.environ.get('WKUBE_STORAGE_CLASS', None)

        self.JOBSTORE_S3_ENDPOINT: Optional[str] = os.environ.get('JOBSTORE_S3_ENDPOINT', None)
        self.JOBSTORE_S3_API_KEY: Optional[str] = os.environ.get('JOBSTORE_S3_API_KEY', None)
        self.JOBSTORE_S3_SECRET_KEY: Optional[str] = os.environ.get('JOBSTORE_S3_SECRET_KEY', None)
        self.JOBSTORE_S3_REGION: Optional[str] = os.environ.get('JOBSTORE_S3_REGION', "eu-central-1")
        self.JOBSTORE_S3_BUCKET_NAME: Optional[str] = os.environ.get('JOBSTORE_S3_BUCKET_NAME', None)

        self.ACCELERATOR_APP_TOKEN: Optional[str] = os.environ.get('ACCELERATOR_APP_TOKEN', None)

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