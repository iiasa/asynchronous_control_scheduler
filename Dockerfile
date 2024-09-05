FROM python:3.11.2

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

COPY . .

CMD celery -A acc_worker.acc_native_jobs worker --loglevel=INFO --concurrency=8