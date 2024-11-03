find /home/nonroot/app -path /home/nonroot/app/.git -prune -o -exec chown nonroot:nonroot {} +

~/.local/bin/celery -A acc_worker.acc_native_jobs worker -B --loglevel=INFO