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
    "docker.io/bitnamilegacy/minio:2025.7.23-debian-12-r3"
    + "@sha256:953d489a81cc4de7975f90e07202189c4325da39b0b92470b6a13c7ea99e36cd"
).with_command("")


minio.env["MINIO_UPDATE"] = "off"
minio.env["MINIO_CALLHOME_ENABLE"] = "off"
minio.env["MINIO_DEFAULT_BUCKETS"] = "test"
minio.env["MINIO_SCHEME"] = "http"
minio.env["MINIO_ROOT_USER"] = "admin"
minio.env["MINIO_ROOT_PASSWORD"] = "miniopass"


delta_spark_builder = ()


@pytest.fixture(scope="session")
def setup_s3(request):
    minio.start()

    def remove_container():
        minio.stop()

    request.addfinalizer(remove_container)


@pytest.fixture(scope="session")
def pathling_fixture(setup_s3):
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "au.csiro.pathling:library-runtime:9.0.0",
                    "io.delta:delta-spark_2.13:4.0.0",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
                    "org.apache.hadoop:hadoop-aws:3.4.1",
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


def test_batch_with_put_and_delete_should_only_retain_latest(
    pathling_fixture, tmp_path
):
    # it might be easiert to just hard-code the data here vs reading from files
    data = [
        {
            "key": "0",
            "value": (
                HERE / "fixtures/resources/batches/put-and-delete/put-0.json"
            ).read_text(),
            "timestamp": datetime.datetime.now(),
            "partition": 0,
            "offset": 0,
        },
        {
            "key": "1",
            "value": (
                HERE / "fixtures/resources/batches/put-and-delete/put-1.json"
            ).read_text(),
            "timestamp": datetime.datetime.now(),
            "partition": 1,
            "offset": 0,
        },
        {
            "key": "2",
            "value": (
                HERE / "fixtures/resources/batches/put-and-delete/put-2.json"
            ).read_text(),
            "timestamp": datetime.datetime.now(),
            "partition": 0,
            "offset": 0,
        },
        {
            "key": "1",
            "value": (
                HERE / "fixtures/resources/batches/put-and-delete/delete-1.json"
            ).read_text(),
            "timestamp": datetime.datetime.now(),
            "partition": 1,
            "offset": 1,
        },
        {
            "key": "1",
            "value": (
                HERE / "fixtures/resources/batches/put-and-delete/put-1.json"
            ).read_text(),
            "timestamp": datetime.datetime.now(),
            "partition": 1,
            "offset": 2,
        },
        {
            "key": "1",
            "value": (
                HERE / "fixtures/resources/batches/put-and-delete/delete-1.json"
            ).read_text(),
            "timestamp": datetime.datetime.now(),
            "partition": 1,
            "offset": 3,
        },
        {
            "key": "2",
            "value": (
                HERE / "fixtures/resources/batches/put-and-delete/put-2-newer.json"
            ).read_text(),
            "timestamp": datetime.datetime.now(),
            "partition": 0,
            "offset": 99,
        },
    ]

    df = pathling_fixture.spark.createDataFrame(data)

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

    assert dt.toDF().count() == 2
    # <https://stackoverflow.com/a/38611657>
    assert [str(row["id"]) for row in dt.toDF().collect()] == ["0", "2"]

    assert dt.toDF().where("id = 2 and active = false").count() == 1, (
        "Expected patient 2 to have active=false after the latest PUT request"
    )
