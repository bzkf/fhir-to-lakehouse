FROM docker.io/library/spark:3.5.4-scala2.12-java17-python3-ubuntu@sha256:f536843be0fef2545bb8db1ce210ccfd6966eff87c9f519922f21cca1cfc4a8b AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_ADDR=0.0.0.0
USER 185:185
WORKDIR /home/spark

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 src/main.py

ENTRYPOINT [ "python3", "src/main.py" ]
