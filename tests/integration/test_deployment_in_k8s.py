import os
from pathlib import Path

from kafka import KafkaProducer
from deltalake import DeltaTable

HERE = Path(os.path.abspath(os.path.dirname(__file__)))


def test_deploy_to_k8s_should_create_delta_tables():
    producer = KafkaProducer(bootstrap_servers="localhost:30092")

    bundle = (
        Path(HERE) / ".." / "unit" / "fixtures" / "resources" / "single-patient.json"
    ).read_bytes()

    producer.send("fhir.msg", bundle)

    producer.flush()

    patient_table_path = "s3://fhir/warehouse/Patient.parquet"
    storage_options = {
        "AWS_ACCESS_KEY_ID": "admin",
        "AWS_SECRET_ACCESS_KEY": "miniopass",
        "AWS_ENDPOINT_URL": "http://localhost:30900",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
    }

    dt = DeltaTable(patient_table_path, storage_options=storage_options)

    assert dt.to_pandas().count() == 1, "Delta table not populated"
