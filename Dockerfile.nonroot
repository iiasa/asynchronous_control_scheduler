FROM ubuntu:22.04

ENV GID=99
ENV UID=1000
ENV STORAGE_DRIVER=overlay
ENV STORAGE_ROOT=/home/nonroot/.local/share/containers/storage

# System dependencies and user setup
RUN apt update && \
    apt install -y sudo pkg-config cmake libtool autoconf python3-pip git && \
    addgroup --gid $GID nonroot && \
    adduser --uid $UID --gid $GID --disabled-password --gecos "" nonroot && \
    echo 'nonroot ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers

# Install Buildah and dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common curl && \
    . /etc/os-release && \
    echo "deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/ /" > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list && \
    curl -L "https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/Release.key" | apt-key add - && \
    apt-get update && \
    apt-get install -y buildah skopeo slirp4netns fuse-overlayfs

# Default registry config
RUN echo "unqualified-search-registries=[\"docker.io\"]" >> /etc/containers/registries.conf

# Setup rootless user mapping
RUN rm -rf /etc/subuid && \
    rm -rf /etc/subgid && \
    echo "nonroot:100000:65536" | tee -a /etc/subuid /etc/subgid

# Switch to non-root user
USER nonroot

# Set up rootless storage directory for Buildah
RUN mkdir -p $STORAGE_ROOT && \
    chmod -R 700 /home/nonroot/.local && \
    chmod -R 700 /home/nonroot/.local/share && \
    chmod -R 700 /home/nonroot/.local/share/containers && \
    chmod -R 700 $STORAGE_ROOT

# (Optional) Trigger storage initialization
RUN buildah info > /dev/null || true

# App setup
RUN mkdir -p /home/nonroot/app
WORKDIR /home/nonroot/app
RUN chmod -R 755 /home/nonroot/app

COPY --chown=nonroot:nonroot ./requirements.txt /home/nonroot/app/requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r /home/nonroot/app/requirements.txt

COPY --chown=nonroot:nonroot . .

CMD ~/.local/bin/celery -A acc_worker.acc_native_jobs worker -B --loglevel=INFO
