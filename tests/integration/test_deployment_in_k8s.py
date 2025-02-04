import os
from pathlib import Path
from confluent_kafka import Producer
from loguru import logger
from deltalake import DeltaTable


HERE = Path(os.path.abspath(os.path.dirname(__file__)))


def test_deploy_to_k8s_should_create_delta_tables():
    # load bundles using this test function
    # check that the delta tables are created (after some time)
    p = Producer({"bootstrap.servers": "localhost:30092"})

    def delivery_report(err, msg):
        if err is not None:
            logger.info("Message delivery failed: {}".format(err))
        else:
            logger.info(
                "Message delivered to {} [{}]".format(msg.topic(), msg.partition())
            )

    bundle = (
        Path(HERE) / ".." / "unit" / "fixtures" / "resources" / "single-patient.json"
    ).read_bytes()

    p.poll(0)

    p.produce("fhir.msg", bundle, callback=delivery_report)

    p.flush()

    patient_table_path = "s3://fhir/warehouse/Patient.parquet"
    storage_options = {
        "AWS_ACCESS_KEY_ID": "admin",
        "AWS_SECRET_ACCESS_KEY": "miniopass",
        "AWS_ENDPOINT_URL": "http://localhost:30900",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
    }

    assert DeltaTable.is_deltatable(
        patient_table_path,
        storage_options=storage_options,
    ), "Delta table not created"

    dt = DeltaTable(patient_table_path, storage_options=storage_options)

    assert dt.to_pandas().count() == 1, "Delta table not populated"
