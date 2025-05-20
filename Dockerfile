FROM ubuntu:22.04

ENV STORAGE_DRIVER=overlay

# Install system dependencies
RUN apt update && \
    apt install -y sudo pkg-config cmake libtool autoconf python3-pip git software-properties-common curl

# Install Buildah and dependencies
RUN . /etc/os-release && \
    echo "deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/ /" > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list && \
    curl -L "https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/Release.key" | apt-key add - && \
    apt-get update && \
    apt-get install -y buildah skopeo

# Optional: Configure default registry behavior
RUN echo "unqualified-search-registries=[\"docker.io\"]" >> /etc/containers/registries.conf

# App setup
RUN mkdir -p /app
WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip3 install --upgrade pip && pip3 install -r /app/requirements.txt

COPY . .

CMD ["celery", "-A", "acc_worker.acc_native_jobs", "worker", "-B", "--loglevel=INFO"]
