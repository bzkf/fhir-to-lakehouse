FROM docker.io/apache/spark:3.5.5-scala2.12-java17-python3-ubuntu@sha256:96dc2bada923824300e1ece564389258934655ba6f463d6daff8753ff0c639ce
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

RUN --mount=type=cache,target=/home/spark/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python3 -m pip install --no-cache-dir -r requirements.txt

COPY src/ src/

USER 185:185
RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 src/main.py

WORKDIR /home/spark
ENTRYPOINT [ "python3", "/opt/fhir-to-delta/src/main.py" ]
