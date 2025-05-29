find /home/ubuntu/app -path /home/ubuntu/app/.git -prune -o -exec chown ubuntu:nonroot {} +

~/.local/bin/celery -A acc_worker.acc_native_jobs worker -B --loglevel=INFO