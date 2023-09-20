from celery import Celery

from accli import ACliService
from .IamcVerificationService import IamcVerificationService

from configs.Environment import get_environment_variables

env = get_environment_variables()


app = Celery('acc_native_jobs', broker=env.CELERY_BROKER_URL)


@app.task
def merge_iamc(*args, **kwargs):
    iamc_verification_service = IamcVerificationService(*args, **kwargs)