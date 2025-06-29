FROM docker.io/apache/spark:3.5.6-scala2.12-java17-python3-ubuntu@sha256:702d82ecea50c6f8344d3df753ba26f05ffd9d1d052e180fed9c3d6c04f77730
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_ADDR=0.0.0.0
WORKDIR /opt/fhir-to-lakehouse
USER root

RUN <<EOF
set -e
mkdir /home/spark
chown -R spark:spark /home/spark
EOF

RUN --mount=type=cache,target=/home/spark/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python3 -m pip install --require-hashes --no-cache-dir -r requirements.txt

COPY src/ src/

USER 185:185
RUN SPARK_INSTALL_PACKAGES_AND_EXIT=1 python3 src/main.py

WORKDIR /home/spark
ENTRYPOINT [ "python3" ]
CMD [ "/opt/fhir-to-lakehouse/src/main.py" ]
