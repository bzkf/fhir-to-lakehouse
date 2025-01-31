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
def pathling_fixture() -> PathlingContext:
    pc = PathlingContext.create(
        None,
        enable_extensions=True,
        enable_delta=True,
        enable_terminology=False,
        terminology_server_url="http://localhost/not-a-real-server",
    )
    return pc


def test_process_batch_with_empty_dataframe_should_not_fail(pathling_fixture):
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


@pytest.mark.parametrize(
    "bundle_path,expected_row_count", [("fixtures/resources/single-patient.json", 1)]
)
def test_process_batch_with_bundle_should_create_table_with_row_count(
    pathling_fixture, tmp_path, bundle_path, expected_row_count
):
    with open(os.path.join(HERE, bundle_path)) as bundle_file:
        bundle = bundle_file.read()

    data = {
        "key": "key",
        "value": bundle,
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

    # if it fails, usually an exception is raised
    bp.process_batch(df, 1)

    dt = DeltaTable.forPath(pathling_fixture.spark, (d / "Patient.parquet").as_posix())

    assert dt.toDF().count() == expected_row_count
