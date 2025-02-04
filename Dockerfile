# https://github.com/ykursadkaya/pyspark-Docker/blob/master/Dockerfile
FROM docker.io/library/spark:3.5.4-scala2.12-java17-python3-ubuntu@sha256:f536843be0fef2545bb8db1ce210ccfd6966eff87c9f519922f21cca1cfc4a8b AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_ADDR=0.0.0.0
USER root
WORKDIR /opt/fhir-to-delta

RUN <<EOF
set -e
mkdir /home/spark
chown -R spark:spark /home/spark
EOF

USER 185:185

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    pip install --no-cache-dir -r requirements.txt

COPY . .

RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 src/main.py

ENTRYPOINT [ "python3", "/opt/fhir-to-delta/src/main.py" ]
