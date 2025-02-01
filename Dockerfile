FROM docker.io/library/spark:3.5.4-scala2.12-java17-python3-ubuntu@sha256:f536843be0fef2545bb8db1ce210ccfd6966eff87c9f519922f21cca1cfc4a8b AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_ADDR=0.0.0.0
WORKDIR /home/spark
USER 185:185

RUN --mount=type=cache,target=/home/spark/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python3 -m pip install --no-cache-dir -r requirements.txt

RUN --mount=type=bind,source=fhir_to_lakehouse/fhir_to_lakehouse.py,target=fhir_to_lakehouse/fhir_to_lakehouse.py \
    SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 fhir_to_lakehouse/fhir_to_lakehouse.py

COPY --chown=185:185 . .

ENTRYPOINT [ "python3", "fhir_to_lakehouse/fhir_to_lakehouse.py" ]
