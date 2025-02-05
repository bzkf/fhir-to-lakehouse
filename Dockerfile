FROM docker.io/apache/spark:3.5.4-scala2.12-java17-python3-ubuntu
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_ADDR=0.0.0.0
WORKDIR /opt/fhir-to-delta
USER root

RUN <<EOF
set -e
mkdir /home/spark
chown -R spark:spark /home/spark
EOF

COPY requirements.txt .

# RUN pip install --no-cache-dir -r requirements.txt

RUN --mount=type=cache,target=/home/spark/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python3 -m pip install --no-cache-dir -r requirements.txt

COPY src/ src/

USER 185:185
RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 src/main.py

ENTRYPOINT [ "python3", "/opt/fhir-to-delta/src/main.py" ]
