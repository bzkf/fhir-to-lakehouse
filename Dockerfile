FROM docker.io/library/spark:3.5.4-scala2.12-java17-python3-ubuntu AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /home/spark
USER 185:185

RUN --mount=type=cache,target=/home/spark/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python3 -m pip install --no-cache-dir -r requirements.txt

COPY . .

# TODO: could cache-mount /home/spark/.ivy2/jars
RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 fhir_to_lakehouse/fhir_to_lakehouse.py

ENTRYPOINT [ "python3", "fhir_to_lakehouse/fhir_to_lakehouse.py" ]
