from click.testing import CliRunner
from lakehousekeeper import vacuum, cli, optimize
from testcontainers.minio import MinioContainer
from pathlib import Path
import os
import pytest
from deltalake import write_deltalake, DeltaTable
import pandas as pd

HERE = Path(os.path.abspath(os.path.dirname(__file__)))

minio = MinioContainer(
    "docker.io/bitnami/minio:2025.6.13-debian-12-r0"
    + "@sha256:ad73a3686271f3082b2cc28e3783b2d499193c3887d863f66cfb0f0256b6fd5d",
    access_key="admin",
    secret_key="miniopass",
).with_command("")


minio.env["MINIO_UPDATE"] = "off"
minio.env["MINIO_CALLHOME_ENABLE"] = "off"
minio.env["MINIO_DEFAULT_BUCKETS"] = "lakehousekeeper-test"
minio.env["MINIO_SCHEME"] = "http"
minio.env["MINIO_ROOT_USER"] = minio.access_key
minio.env["MINIO_ROOT_PASSWORD"] = minio.secret_key


delta_spark_builder = ()


@pytest.fixture(scope="session")
def setup_s3(request):
    minio.start()

    def remove_container():
        minio.stop()

    request.addfinalizer(remove_container)


@pytest.fixture
def setup_env(monkeypatch):
    monkeypatch.setenv(
        "AWS_ENDPOINT_URL", f"http://localhost:{minio.get_exposed_port(9000)}"
    )
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", minio.secret_key)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", minio.access_key)


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0


def test_vacuum(setup_s3, setup_env):
    client = minio.get_client()
    client.make_bucket("test-vacuum")

    storage_options = {
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "0",
        "AWS_ALLOW_HTTP": "1",
    }

    df = pd.DataFrame({"id": [1, 2, 3]})
    write_deltalake(
        "s3://test-vacuum/default/Patient.parquet",
        df,
        storage_options=storage_options,
    )

    dt = DeltaTable(
        "s3://test-vacuum/default/Patient.parquet", storage_options=storage_options
    )

    assert len(dt.to_pandas().index) == 3

    runner = CliRunner()
    result = runner.invoke(
        vacuum,
        [
            "--bucket-name=test-vacuum",
            "--database-name-prefix=default",
            "--retention-hours=0",
            "--enforce-retention-duration=False",
            "--dry-run=False",
            "--use-delta-rs=False",
        ],
    )

    assert result.exit_code == 0


def test_optimize(setup_s3, setup_env):
    client = minio.get_client()
    client.make_bucket("test-optimize")

    storage_options = {
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "0",
        "AWS_ALLOW_HTTP": "1",
    }

    df = pd.DataFrame({"id": [1, 2, 3]})
    write_deltalake(
        "s3://test-optimize/default/Patient.parquet",
        df,
        storage_options=storage_options,
    )

    dt = DeltaTable(
        "s3://test-optimize/default/Patient.parquet", storage_options=storage_options
    )

    assert len(dt.to_pandas().index) == 3

    runner = CliRunner()
    result = runner.invoke(
        optimize,
        [
            "--bucket-name=test-optimize",
            "--database-name-prefix=default",
        ],
    )

    assert result.exit_code == 0
