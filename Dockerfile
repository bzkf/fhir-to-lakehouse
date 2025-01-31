FROM docker.io/library/spark:3.5.4-scala2.12-java17-python3-ubuntu@sha256:4133c4efd3731f87f13d107ade3f9a98370a19a6561fad7d95fd3bd06019a864 AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_ADDR=0.0.0.0
WORKDIR /home/spark
USER 185:185

RUN --mount=type=cache,target=/home/spark/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python3 -m pip install --no-cache-dir -r requirements.txt

COPY --chown=185:185 . .

RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 src/main.py

ENTRYPOINT [ "python3", "src/main.py" ]
