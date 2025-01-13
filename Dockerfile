FROM docker.io/library/spark:3.5.1-scala2.12-java17-python3-ubuntu AS base
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /home/spark

RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python3 -m pip install -r requirements.txt

COPY . .

ENTRYPOINT [ "python3", "fhir_to_lakehouse/fhir_to_lakehouse.py" ]
