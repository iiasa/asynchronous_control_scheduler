from configs.Environment import get_environment_variables

env = get_environment_variables()

broker_url = env.CELERY_BROKER_URL

task_routes = {'dispatch_wkube_task': {'queue': 'wkube'}}
task_time_limit = 60 * 60