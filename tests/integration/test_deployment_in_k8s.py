import hashlib
import os
import time
from pathlib import Path

from deltalake import DeltaTable
import polars as pl
from kafka import KafkaProducer
from loguru import logger

HERE = Path(os.path.abspath(os.path.dirname(__file__)))


def test_deploy_to_k8s_should_create_delta_tables():
    producer = KafkaProducer(
        bootstrap_servers="localhost:30090",
    )

    bundle = (
        Path(HERE)
        / ".."
        / "unit"
        / "fixtures"
        / "resources"
        / "put-100-patients.ndjson"
    ).read_text()

    for line in bundle.splitlines():
        if line.strip():  # Skip empty lines
            line_hash = hashlib.sha256(line.encode()).hexdigest().encode()
            producer.send(
                topic="fhir.msg",
                value=line.encode("utf-8"),
                key=line_hash,
            )
    producer.flush(timeout=60)

    logger.info("Sent bundle to Kafka")

    patient_table_path = "s3://fhir/warehouse/Patient.parquet"
    condition_table_path = "s3://fhir/warehouse/Condition.parquet"
    storage_options = {
        "AWS_ACCESS_KEY_ID": "admin",
        "AWS_SECRET_ACCESS_KEY": "miniopass",
        "AWS_ENDPOINT_URL": "http://localhost:30900",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        "ALLOW_HTTP": "true",
    }

    time.sleep(30)

    for attempt in range(5):
        try:
            dt = DeltaTable(patient_table_path, storage_options=storage_options)

            assert (
                len(dt.to_pandas(columns=["id"])) == 121
            ), "Unexpected number of rows in Patient table"

            dt = DeltaTable(condition_table_path, storage_options=storage_options)
            assert (
                len(dt.to_pandas(columns=["id"])) == 5416
            ), "Unexpected number of rows in Condition table"
            break
        except Exception as e:
            logger.warning(
                "Attempt {attempt} failed: {error}", attempt=attempt + 1, error=e
            )
            if attempt < 4:  # Don't sleep on last attempt
                time.sleep(2**attempt)
            else:
                raise e
