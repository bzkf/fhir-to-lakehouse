import datetime
import os
from pathlib import Path

import pytest
from delta import DeltaTable
from pathling import PathlingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, StructField, StructType
from testcontainers.minio import MinioContainer

from bundle_processor import BundleProcessor
from settings import (
    DeltaSettings,
    KafkaSettings,
    KafkaSslSettings,
    Settings,
    SparkSettings,
)

HERE = Path(os.path.abspath(os.path.dirname(__file__)))

minio = MinioContainer(
    "docker.io/bitnami/minio:2025.3.12"
    + "@sha256:7c92dd1ba1f48e1009079c5e3f0a98e3c5a34387fc474007f1a887db7643e2c2"
).with_command("")


minio.env["MINIO_UPDATE"] = "off"
minio.env["MINIO_CALLHOME_ENABLE"] = "off"
minio.env["MINIO_DEFAULT_BUCKETS"] = "test"
minio.env["MINIO_SCHEME"] = "http"
minio.env["MINIO_ROOT_USER"] = "admin"
minio.env["MINIO_ROOT_PASSWORD"] = "miniopass"


delta_spark_builder = ()


@pytest.fixture(scope="module")
def setup_s3(request):
    minio.start()

    def remove_container():
        minio.stop()

    request.addfinalizer(remove_container)


@pytest.fixture
def pathling_fixture(setup_s3):
    spark = (
        SparkSession.builder.config(
            "spark.jars.packages",
            ",".join(
                [
                    "au.csiro.pathling:library-runtime:7.0.1",
                    "io.delta:delta-spark_2.12:3.2.0",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                ]
            ),
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"localhost:{minio.get_exposed_port(9000)}",
        )
        .config("fs.s3a.committer.name", "magic")
        .config("fs.s3a.committer.magic.enabled", "true")
        .config("fs.s3a.access.key", minio.env["MINIO_ROOT_USER"])
        .config("fs.s3a.secret.key", minio.env["MINIO_ROOT_PASSWORD"])
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .getOrCreate()
    )
    pc = PathlingContext.create(
        spark,
        enable_extensions=True,
        enable_delta=True,
        enable_terminology=False,
        terminology_server_url="http://localhost/not-a-real-server",
    )
    return pc


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
            delta=DeltaSettings(),
            spark=SparkSettings(),
            kafka=KafkaSettings(ssl=KafkaSslSettings()),
        ),
    )

    # if it fails, usually an exception is raised
    assert bp.process_batch(empty_df, 0) is None


def test_delete_afer_insert_should_delete_row(pathling_fixture, tmp_path):
    put_bundle = (HERE / "fixtures/resources/single-patient.json").read_text()

    data = {
        "key": "key",
        "value": put_bundle,
        "timestamp": datetime.datetime.now(),
        "partition": 0,
        "offset": 0,
    }

    df = pathling_fixture.spark.createDataFrame([data])

    d = tmp_path / "warehouse" / "data"
    settings = Settings(
        delta_database_dir=d.as_posix(),
        spark=SparkSettings(),
        delta=DeltaSettings(),
        kafka=KafkaSettings(ssl=KafkaSslSettings()),
    )

    bp = BundleProcessor(pathling_fixture, settings=settings)

    df = bp.prepare_stream(df)

    bp.process_batch(df, 1)

    dt = DeltaTable.forPath(pathling_fixture.spark, (d / "Patient.parquet").as_posix())

    assert dt.toDF().count() == 1
    assert dt.toDF().first().id == "cd30dceb-20c8-1e15-ad0c-c9fe2a48ea4e"

    delete_bundle = (HERE / "fixtures/resources/delete-single-patient.json").read_text()

    data = {
        "key": "key",
        "value": delete_bundle,
        "timestamp": datetime.datetime.now(),
        "partition": 0,
        "offset": 1,
    }

    df = pathling_fixture.spark.createDataFrame([data])

    df = bp.prepare_stream(df)

    bp.process_batch(df, 2)

    assert dt.toDF().count() == 0


def test_store_tables_in_minio(pathling_fixture):
    put_bundle = (HERE / "fixtures/resources/single-patient.json").read_text()

    settings = Settings(
        delta_database_dir="s3a://test/data",
        spark=SparkSettings(
            checkpoint_dir="s3a://test/checkpoint",
        ),
        delta=DeltaSettings(),
        kafka=KafkaSettings(ssl=KafkaSslSettings()),
    )

    data = {
        "key": "key",
        "value": put_bundle,
        "timestamp": datetime.datetime.now(),
        "partition": 0,
        "offset": 0,
    }

    df = pathling_fixture.spark.createDataFrame([data])

    bp = BundleProcessor(pathling_fixture, settings=settings)

    df = bp.prepare_stream(df)

    bp.process_batch(df, 1)

    dt = DeltaTable.forPath(pathling_fixture.spark, "s3a://test/data/Patient.parquet")

    assert dt.toDF().count() == 1
    assert dt.toDF().first().id == "cd30dceb-20c8-1e15-ad0c-c9fe2a48ea4e"


def test_vaccuum_and_optimize(pathling_fixture, tmp_path):
    put_bundle = (HERE / "fixtures/resources/single-patient.json").read_text()

    data = {
        "key": "key",
        "value": put_bundle,
        "timestamp": datetime.datetime.now(),
        "partition": 0,
        "offset": 0,
    }

    df = pathling_fixture.spark.createDataFrame([data])

    d = tmp_path / "warehouse" / "data"
    settings = Settings(
        delta_database_dir=d.as_posix(),
        spark=SparkSettings(),
        delta=DeltaSettings(),
        kafka=KafkaSettings(ssl=KafkaSslSettings()),
    )

    bp = BundleProcessor(pathling_fixture, settings=settings)

    df = bp.prepare_stream(df)

    # batch_id of 0 already triggers the default upkeep interval
    bp.process_batch(df, 0)

    dt = DeltaTable.forPath(pathling_fixture.spark, (d / "Patient.parquet").as_posix())

    assert dt.toDF().count() == 1
    assert dt.toDF().first().id == "cd30dceb-20c8-1e15-ad0c-c9fe2a48ea4e"


def test_liquid_clustering(pathling_fixture, tmp_path):
    put_bundle = (HERE / "fixtures/resources/single-patient.json").read_text()

    data = {
        "key": "key",
        "value": put_bundle,
        "timestamp": datetime.datetime.now(),
        "partition": 0,
        "offset": 0,
    }

    df = pathling_fixture.spark.createDataFrame([data])

    clustering_columns_by_resource_type = {
        "Patient": ["id", "birthDate"],
    }

    d = tmp_path / "warehouse" / "data"
    settings = Settings(
        delta_database_dir=d.as_posix(),
        spark=SparkSettings(),
        delta=DeltaSettings(
            clustering_columns_by_resource_type=clustering_columns_by_resource_type
        ),
        kafka=KafkaSettings(ssl=KafkaSslSettings()),
    )

    bp = BundleProcessor(pathling_fixture, settings=settings)

    df = bp.prepare_stream(df)

    # batch_id of 0 already triggers the default upkeep interval
    # optimize triggers liquid clustering
    bp.process_batch(df, 0)

    dt = DeltaTable.forPath(pathling_fixture.spark, (d / "Patient.parquet").as_posix())

    assert dt.toDF().count() == 1
    assert dt.toDF().first().id == "cd30dceb-20c8-1e15-ad0c-c9fe2a48ea4e"
