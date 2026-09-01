# fhir-to-lakehouse

[![OpenSSF Scorecard](https://img.shields.io/ossf-scorecard/github.com/bzkf/fhir-to-lakehouse?label=openssf%20scorecard&style=flat)](https://scorecard.dev/viewer/?uri=github.com/bzkf/fhir-to-lakehouse)

Reads FHIR bundles from Kafka, encodes them using the Pathling encoders, and upserts them into Delta Lake or Iceberg tables.

## Configuration

The application uses the [Typed Settings](https://typed-settings.readthedocs.io/en/latest/) library, so you can
configure everything in the [settings.py](./src/settings.py) using [environment variables](https://typed-settings.readthedocs.io/en/latest/guides/environment-variables.html).

For the nested configuration it is easier to use a [config file](https://typed-settings.readthedocs.io/en/latest/guides/config-files.html):
For example, create the followint settings.toml file:

```toml
[fhir-to-lakehouse]
table_format = "delta" # or "iceberg"

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

### Table Format: Delta Lake vs. Iceberg

By default, resources are upserted into [Delta Lake](https://delta.io/) tables located under `delta_database_dir`
(one `<resource_type>.parquet` folder per FHIR resource type).

Set `table_format = "iceberg"` (or the env var `FHIR_TO_LAKEHOUSE_TABLE_FORMAT=iceberg`) to upsert into
[Apache Iceberg](https://iceberg.apache.org/) tables instead. Iceberg tables are registered in the Spark catalog
configured via the `iceberg` settings (`iceberg.catalog_name`, default `iceberg`) and are stored under
`iceberg_database_dir`. By default, a Hadoop catalog is used (`iceberg.catalog_type = "hadoop"`), which doesn't
require a Hive metastore; set `iceberg.catalog_type = "hive"` together with `metastore_url` to register Iceberg
tables in a Hive metastore instead. Table maintenance (file compaction via `rewrite_data_files` and old-snapshot
expiry via `expire_snapshots`) runs periodically, just like `OPTIMIZE`/`VACUUM` do for Delta tables.

Both Delta and Iceberg jars are always installed, regardless of `table_format`, so switching formats doesn't
require re-downloading packages at container startup.

#### Tuning Iceberg tables for large, continuously-upserted resource types

Iceberg's default `write.merge.mode`/`write.update.mode`/`write.delete.mode` is copy-on-write, which rewrites an
entire data file whenever a single row in it changes. `IcebergSettings` defaults these to `merge-on-read` instead
(`iceberg.write_merge_mode`, `iceberg.write_update_mode`, `iceberg.write_delete_mode`), which is much cheaper for
the continuous small-batch upserts this application does: each batch just writes small delete/data files instead
of rewriting whatever files it touches. Periodic compaction (`rewrite_data_files`, run automatically every
`spark.upkeep_interval` batches) keeps the resulting read-time delete-file overhead bounded.

For resource types with a high-cardinality merge key and a lot of rows (e.g. `Observation`, whose `id` is
typically a content hash/UUID with no natural range locality), also configure hash-bucket partitioning on that
key via `iceberg.bucket_column_by_resource_type` / `iceberg.bucket_count_by_resource_type`, and a sort order via
`iceberg.sort_columns_by_resource_type`:

```toml
[fhir-to-lakehouse.iceberg.bucket_column_by_resource_type]
Observation = "id"

[fhir-to-lakehouse.iceberg.bucket_count_by_resource_type]
Observation = 128

[fhir-to-lakehouse.iceberg.sort_columns_by_resource_type]
Observation = ["id"]
```

Bucketing on `id` deterministically confines every row to exactly one of N partitions, so a MERGE only ever has
to consider the partitions its batch's ids hash into — this holds regardless of how well the rest of the table
happens to be clustered, which matters for a hash-like key where a plain sort order alone doesn't create range
locality. The sort order is applied locally per write task on every batch, and fully enforced across files during
periodic maintenance (which switches to `rewrite_data_files(..., strategy => 'sort')` automatically once
`sort_columns_by_resource_type` is set for that resource type).

There's no universal bucket count — size it so each bucket ends up around one-to-a-few `write.target-file-size-bytes`
files once the table matures: `bucket_count ≈ expected_total_data_size / (write.target_file_size_bytes × files_per_bucket)`.
For "several hundred million" `Observation` rows, a starting point in the range of 64–256 (pick a power of 2) is
reasonable; check the average file size after a few compaction passes and adjust if files consistently end up far
from the target size.

### Spark Config

By default, the `SPARK_CONF_DIR` environment variable inside the container is set to `/app/spark/conf`, so you
can mount a `spark-defaults.conf` file at `/app/spark/conf/spark-defaults.conf` to override any Spark setting.

## Lakehousekeeper

A CLI tool called `lakehousekeeper` is also part of the container distribution.
It implements commands for vacuuming, optimizing, and registering tables from S3-compatible object storage.
You can invoke it by running:

<!-- x-release-please-start-version -->

```sh
docker run --rm -it ghcr.io/bzkf/fhir-to-lakehouse:v1.13.16 /opt/fhir-to-lakehouse/src/lakehousekeeper.py -- --help
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
