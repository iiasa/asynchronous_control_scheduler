FROM ubuntu:24.04

ENV GID=99
ENV UID=1000
ENV STORAGE_DRIVER=overlay
ENV STORAGE_ROOT=/home/ubuntu/.local/share/containers/storage

RUN apt update && \
    apt install -y sudo pkg-config cmake libtool autoconf python3-pip git software-properties-common curl buildah skopeo fuse-overlayfs slirp4netns

RUN passwd -l ubuntu
RUN addgroup --gid $GID nonroot
RUN usermod -aG nonroot ubuntu
RUN echo 'ubuntu ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers

# Setup containers config
RUN cat <<EOF > /etc/containers/registries.conf 
unqualified-search-registries = [
  "docker.io",
]

[[registry]]
location="docker.io"
    [[registry.mirror]]
    location="mirror.gcr.io"

[engine]
short-name-mode = "permissive"
EOF

# Setup subuid/gid for rootless buildah
RUN rm -rf /etc/subuid && \
    rm -rf /etc/subgid && \
    echo "ubuntu:100000:65536" | tee -a /etc/subuid /etc/subgid

# Switch to ubuntu user
USER ubuntu

# Create and set proper rootless storage directory
RUN mkdir -p /home/ubuntu/.local/share/containers/storage && \
    chmod -R 700 /home/ubuntu/.local && \
    chmod -R 700 /home/ubuntu/.local/share && \
    chmod -R 700 /home/ubuntu/.local/share/containers && \
    chmod -R 700 /home/ubuntu/.local/share/containers/storage

# Buildah storage config check (optional, triggers initialization)
RUN buildah info > /dev/null || true

RUN mkdir /home/ubuntu/app
WORKDIR /home/ubuntu/app

RUN chmod -R 755 /home/ubuntu/app

COPY --chown=ubuntu:nonroot ./requirements /home/ubuntu/app/requirements

RUN pip3 install --upgrade pip --break-system-packages
RUN pip3 install -r /home/ubuntu/app/requirements/prod.txt --break-system-packages

COPY --chown=ubuntu:nonroot . .

# The entrypoint will be overridden by the K8s Job command
CMD ["python3", "-m", "acc_worker.k8_gateway_actions.dispatch_build_and_push"]
