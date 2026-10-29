import datetime
import os
from pathlib import Path

import pytest
from delta import DeltaTable
from pathling import PathlingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, StructField, StructType

from bundle_processor import BundleProcessor
from settings import (
    DeltaSettings,
    KafkaSettings,
    KafkaSslSettings,
    Settings,
    SparkSettings,
)
from spark_jars import with_jars
from testcontainers.core.container import DockerContainer

HERE = Path(os.path.abspath(os.path.dirname(__file__)))

seaweedfs = (
    DockerContainer("docker.io/chrislusf/seaweedfs:latest")
    .with_command("mini -dir=/data")
    .with_exposed_ports(8333)
    .with_env("AWS_ACCESS_KEY_ID", "admin")
    .with_env("AWS_SECRET_ACCESS_KEY", "seaweedpass")
    .with_env("S3_BUCKET", "test")
    .with_kwargs(user="1000:1000")
)

delta_spark_builder = ()


@pytest.fixture(scope="session")
def setup_s3(request):
    seaweedfs.start()

    def remove_container():
        seaweedfs.stop()

    request.addfinalizer(remove_container)


@pytest.fixture(scope="session")
def pathling_fixture(setup_s3):
    spark = (
        with_jars(SparkSession.builder.master("local[*]"))
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
            f"localhost:{seaweedfs.get_exposed_port(8333)}"
        )
        .config("fs.s3a.committer.name", "magic")
        .config("fs.s3a.committer.magic.enabled", "true")
        .config("fs.s3a.access.key", seaweedfs.env["AWS_ACCESS_KEY_ID"])
        .config("fs.s3a.secret.key", seaweedfs.env["AWS_SECRET_ACCESS_KEY"])
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
    first_row = dt.toDF().first()
    assert first_row is not None
    assert first_row.id == "cd30dceb-20c8-1e15-ad0c-c9fe2a48ea4e"

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


def test_store_tables_in_seaweedfs(pathling_fixture):
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
    first_row = dt.toDF().first()
    assert first_row is not None
    assert first_row.id == "cd30dceb-20c8-1e15-ad0c-c9fe2a48ea4e"


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
    first_row = dt.toDF().first()
    assert first_row is not None
    assert first_row.id == "cd30dceb-20c8-1e15-ad0c-c9fe2a48ea4e"


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
    first_row = dt.toDF().first()
    assert first_row is not None
    assert first_row.id == "cd30dceb-20c8-1e15-ad0c-c9fe2a48ea4e"


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
    assert sorted(str(row["id"]) for row in dt.toDF().collect()) == ["0", "2"]

    assert dt.toDF().where("id = 2 and active = false").count() == 1, (
        "Expected patient 2 to have active=false after the latest PUT request"
    )
