import datetime
import os

import pytest
from delta import DeltaTable
from pathling import PathlingContext
from pyspark.sql.types import (
    BinaryType,
    StructField,
    StructType,
)

from bundle_processor import BundleProcessor
from settings import KafkaSettings, Settings, SparkSettings

HERE = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def pathling_fixture():
    pc = PathlingContext.create(
        None,
        enable_extensions=True,
        enable_delta=True,
        enable_terminology=False,
        terminology_server_url="http://localhost/not-a-real-server",
    )
    yield pc


def test_with_empty_dataframe_should_not_fail(pathling_fixture):
    schema = StructType(
        [
            StructField("key", BinaryType(), True),
            StructField("value", BinaryType(), True),
        ]
    )
    empty_df = pathling_fixture.spark.createDataFrame([], schema)
    bp = BundleProcessor(
        pathling_fixture,
        settings=Settings(
            spark=SparkSettings(),
            kafka=KafkaSettings(),
        ),
    )

    # if it fails, usually an exception is raised
    assert bp.process_batch(empty_df, 0) is None


def test_delete_afer_insert_should_delete_row(pathling_fixture, tmp_path):
    with open(
        os.path.join(HERE, "fixtures/resources/single-patient.json")
    ) as bundle_file:
        put_bundle = bundle_file.read()

    data = {
        "key": "key",
        "value": put_bundle,
        "timestamp": datetime.datetime.now(),
        "partition": 0,
        "offset": 0,
    }

    df = pathling_fixture.spark.createDataFrame([data])

    d = tmp_path / "fhir-to-delta-warehouse" / "data"
    settings = Settings(
        delta_database_dir=d.as_posix(),
        spark=SparkSettings(),
        kafka=KafkaSettings(),
    )

    bp = BundleProcessor(pathling_fixture, settings=settings)

    bp.process_batch(df, 1)

    dt = DeltaTable.forPath(pathling_fixture.spark, (d / "Patient.parquet").as_posix())

    assert dt.toDF().count() == 1
    assert dt.toDF().first().id == "cd30dceb-20c8-1e15-ad0c-c9fe2a48ea4e"

    with open(
        os.path.join(HERE, "fixtures/resources/delete-single-patient.json")
    ) as bundle_file:
        delete_bundle = bundle_file.read()

    data = {
        "key": "key",
        "value": delete_bundle,
        "timestamp": datetime.datetime.now(),
        "partition": 0,
        "offset": 1,
    }

    df = pathling_fixture.spark.createDataFrame([data])

    bp.process_batch(df, 2)

    assert dt.toDF().count() == 0
