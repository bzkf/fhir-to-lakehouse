# fhir-to-lakehouse

[![OpenSSF Scorecard](https://img.shields.io/ossf-scorecard/github.com/bzkf/fhir-to-lakehouse?label=openssf%20scorecard&style=flat)](https://scorecard.dev/viewer/?uri=github.com/bzkf/fhir-to-lakehouse)

Reads FHIR bundles from Kafka, encodes them using the Pathling encoders, and stores them as Delta Lake tables.

## Configuration

The application uses the [Typed Settings](https://typed-settings.readthedocs.io/en/latest/) library, so you can
configure everything in the [settings.py](./src/settings.py) using [environment variables](https://typed-settings.readthedocs.io/en/latest/guides/environment-variables.html).

For the nested configuration it is easier to use a [config file](https://typed-settings.readthedocs.io/en/latest/guides/config-files.html):
For example, create the followint settings.toml file:

```toml
[fhir-to-lakehouse.delta]
checkpoint_interval = "123"

[fhir-to-lakehouse.delta.clustering_columns_by_resource_type]
Patient = ["id", "birthDate"]
Observation = ["id", "effectiveDateTime", "subject"]
Condition = ["id", "recordedDate", "onsetDateTime", "subject"]
```

and start the application with the env var `FHIR_TO_LAKEHOUSE_SETTINGS` pointing to this file:

```sh
FHIR_TO_LAKEHOUSE_SETTINGS=settings.toml python src/main.py
```

### Spark Config

By default, the `SPARK_CONF_DIR` environment variable inside the container is set to `/app/spark/conf`, so you
can mount a `spark-defaults.conf` file at `/app/spark/conf/spark-defaults.conf` to override any Spark setting.

## Lakehousekeeper

A CLI tool called `lakehousekeeper` is also part of the container distribution.
It implements commands for vacuuming, optimizing, and registering tables from S3-compatible object storage.
You can invoke it by running:

<!-- x-release-please-start-version -->

```sh
docker run --rm -it ghcr.io/bzkf/fhir-to-lakehouse:v1.13.5 /opt/fhir-to-lakehouse/src/lakehousekeeper.py -- --help
```

<!-- x-release-please-end-version -->

## Development

Install `uv` <https://docs.astral.sh/uv/getting-started/installation/#installation-methods>:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Install dependencies using

```sh
uv sync
```

The [compose.yaml](compose.yaml) contains the development fixtures required to run the program out-of-the-box:

- Apache Kafka (Exposed on <127.0.0.1:9094>)
- AKHQ - an Apache Kafka UI (Exposed on <127.0.0.1:8084>)
- MinIO (Exposed on <127.0.0.1:9000> and <127.0.0.1:9001> for the UI)
- mock-data-loader: used to pre-load Kafka with sample FHIR bundles

start all services using

```sh
docker compose up
```

and the program itself using

```sh
uv run src/main.py
```

### Tests

#### Unit Tests

```sh
uv run pytest --cov=src tests/unit/
```

#### Integration Tests

Currently, running these tests outside of the CI requires some manual effort:

```sh
kind create cluster --config=tests/integration/kind-config.yaml

docker build -t ghcr.io/bzkf/fhir-to-lakehouse:test .
kind load docker-image ghcr.io/bzkf/fhir-to-lakehouse:test

helm dep up tests/integration/fixtures/
helm upgrade --install --wait fixtures tests/integration/fixtures/
helm upgrade --install --wait --set "stream-processors.enabled=true" --set "stream-processors.processors.fhir-to-delta.container.image.tag=test" fixtures tests/integration/fixtures/
```

To run the integration tests

```sh
uv run pytest tests/integration
```

To check the table counts

```sh
duckdb -no-stdin -init tests/integration/check-counts.sql
```
