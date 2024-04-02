from configs.Environment import get_environment_variables

env = get_environment_variables()

broker_url = env.CELERY_BROKER_URL

task_routes = {'acc_native_jobs.dispatch_wkube_task': {'queue': 'wkube'}}