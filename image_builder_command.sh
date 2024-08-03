find /home/nonroot/app -path /home/nonroot/app/.git -prune -o -exec chown nonroot:nonroot {} +

cd ~/app && ~/.local/bin/celery -A acc_native_jobs worker -Q wkube -B --loglevel=INFO