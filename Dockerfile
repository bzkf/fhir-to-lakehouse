
FROM docker.io/curlimages/curl:8.20.0@sha256:b3f1fb2a51d923260350d21b8654bbc607164a987e2f7c84a0ac199a67df812a AS blazectl
SHELL ["/bin/sh", "-eo", "pipefail", "-c"]
USER root
RUN <<EOF
set -e
curl -LSs "https://github.com/samply/blazectl/releases/download/v1.4.0/blazectl-1.4.0-linux-amd64.tar.gz" | tar xz
mv blazectl /usr/local/bin/blazectl
chmod +x /usr/local/bin/blazectl
blazectl --version
EOF

FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim@sha256:82f018bb3bd8b1d12c376c3e87da186ec1932cbf91bc8e73089feea6428fec00
SHELL ["/bin/bash", "-eo", "pipefail", "-c"]
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_ADDR=0.0.0.0 \
    SPARK_CONF_DIR=/app/spark/conf

WORKDIR /app

# hadolint ignore=DL3008
RUN <<EOF
apt-get update -y
apt-get install -y --no-install-recommends openjdk-21-jre-headless
rm -rf /var/lib/apt/lists/*
apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false

groupadd -r -g 65532 nonroot
useradd --create-home --shell /bin/bash --uid 65532 -g nonroot nonroot
EOF

COPY --from=blazectl /usr/local/bin/blazectl /usr/local/bin/blazectl

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Copy the project into the image
COPY . /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"

USER 65532:65532

RUN <<EOF
set -e
SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 src/main.py
rm -rf /tmp/spark*
EOF

ENTRYPOINT [ "python3", "src/main.py" ]
