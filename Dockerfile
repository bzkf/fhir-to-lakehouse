FROM docker.io/library/python:3.13-slim-trixie@sha256:70729b46c69b4f1e97c4822c1af3df53a1476cf5ddc6c087c0c10bc3a5678c2f AS base
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# hadolint ignore=DL3008
RUN <<EOF
set -e
apt-get update -y
apt-get install -y --no-install-recommends openjdk-21-jre-headless
rm -rf /var/lib/apt/lists/*
apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false

groupadd -r -g 65532 nonroot
useradd --create-home --shell /bin/bash --uid 65532 -g nonroot nonroot
EOF

FROM base AS build
COPY --from=ghcr.io/astral-sh/uv:0.11.26@sha256:3d868e555f8f1dbc324afa005066cd11e1053fc4743b9808ca8025283e65efa5 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Download the Spark packages. Only the resulting jars are copied to the final image,
# not the Ivy cache they are resolved from.
RUN --mount=type=bind,source=src/spark_jars.py,target=/app/src/spark_jars.py <<EOF
set -e
/app/.venv/bin/python src/spark_jars.py /opt/ivy
rm -rf /tmp/spark*
EOF

FROM base
ENV METRICS_ADDR=0.0.0.0 \
    SPARK_CONF_DIR=/app/spark/conf \
    SPARK_JARS_DIR=/app/jars \
    PATH="/app/.venv/bin:$PATH"

COPY --from=build /opt/ivy/jars /app/jars
COPY --from=build /app/.venv /app/.venv
COPY src /app/src

USER 65532:65532

ENTRYPOINT [ "python3", "src/main.py" ]
