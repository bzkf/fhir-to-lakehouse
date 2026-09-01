# fhir-to-lakehouse

[![OpenSSF Scorecard](https://img.shields.io/ossf-scorecard/github.com/bzkf/fhir-to-lakehouse?label=openssf%20scorecard&style=flat)](https://scorecard.dev/viewer/?uri=github.com/bzkf/fhir-to-lakehouse)

Reads FHIR bundles from Kafka, encodes them using the Pathling encoders, and upserts them into Delta Lake or Iceberg tables.

## Configuration

The application uses the [Typed Settings](https://typed-settings.readthedocs.io/en/latest/) library, so you can
configure everything in the [settings.py](./src/settings.py) using [environment variables](https://typed-settings.readthedocs.io/en/latest/guides/environment-variables.html).

For the nested configuration it is easier to use a [config file](https://typed-settings.readthedocs.io/en/latest/guides/config-files.html):
For example, create the following settings.toml file:

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

`iceberg.write_merge_mode` / `iceberg.write_update_mode` / `iceberg.write_delete_mode` default to `copy-on-write`
(Iceberg's own default), which keeps reads/analytics fast since queries never have to merge delete files at scan
time — the cost is that every batch rewrites whole data files for any row it touches. Switch these to
`merge-on-read` instead for a resource type where write/ingestion throughput is the bottleneck rather than query
latency: each batch then only writes small delete/data files, at the cost of scans having to merge those delete
files in until the next compaction pass (`rewrite_data_files`, run automatically every `spark.upkeep_interval`
batches) catches up.

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

## Monitoring

Prometheus metrics are exposed on port `8000` at `/metrics` (configurable via `METRICS_PORT` and `METRICS_ADDR`).
Besides the application metrics, the endpoint also includes the driver's JVM memory and GC metrics and Spark's task,
storage memory, and shuffle metrics, so Spark's own UI port doesn't need to be scraped.

| Metric                                                                                   | Description                                                                               |
| ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `resources_processed_total`                                                              | resources written or deleted, by resource type                                            |
| `delta_merge_rows_total`                                                                 | rows inserted, updated, deleted, or copied unchanged by Delta MERGE, as reported by Delta |
| `delta_merge_phase_duration_seconds`                                                     | duration of the `materialize_source`, `scan`, and `rewrite` phases of each Delta MERGE    |
| `delta_merge_files_total`, `delta_merge_bytes_total`                                     | files and bytes added and removed by Delta MERGE                                          |
| `delta_table_size_bytes`, `delta_table_files`                                            | size and number of files of each Delta table                                              |
| `delta_operation_duration_seconds`                                                       | duration of Delta operations (merge, optimize, vacuum, register)                          |
| `spark_streaming_query_input_rows_total`                                                 | Kafka records read by streaming query                                                     |
| `spark_streaming_query_batch_duration_seconds`                                           | duration of non-empty micro-batches by streaming query                                    |
| `spark_streaming_query_trigger_phase_duration_seconds`                                   | duration of each phase of the latest trigger, e.g. `addBatch`                             |
| `spark_streaming_query_kafka_offset`                                                     | start, end, and latest Kafka offsets, used to calculate the lag                           |
| `jvm_memory_{used,committed,limit,used_after_last_gc}_bytes`                             | JVM memory by memory pool                                                                 |
| `jvm_gc_collection_time_seconds_total`, `jvm_gc_collections_total`                       | GC time and count by collector                                                            |
| `jvm_process_resident_memory_bytes`                                                      | resident memory of the JVM process, including off-heap memory                             |
| `spark_executor_tasks_total`, `spark_executor_active_tasks`                              | finished and running Spark tasks                                                          |
| `spark_executor_storage_memory_{used,limit}_bytes`, `spark_executor_shuffle_bytes_total` | cached data and shuffle data                                                              |

The [grafana/fhir-to-lakehouse.json](grafana/fhir-to-lakehouse.json) dashboard visualizes these metrics. Import it
into Grafana and select your Prometheus data source. The panel descriptions explain what to look out for.

## Lakehousekeeper

A CLI tool called `lakehousekeeper` is also part of the container distribution.
It implements commands for vacuuming, optimizing, and registering tables from S3-compatible object storage.
You can invoke it by overriding the container's entrypoint:

<!-- x-release-please-start-version -->

```sh
docker run --rm -it --entrypoint python3 ghcr.io/bzkf/fhir-to-lakehouse:v1.16.0 src/lakehousekeeper.py --help
```

For example, to list the files a VACUUM of all tables in `s3://fhir/warehouse/` would delete:

```sh
docker run --rm -it \
  -e AWS_ENDPOINT_URL=http://seaweedfs:8333 \
  -e AWS_ACCESS_KEY_ID=admin \
  -e AWS_SECRET_ACCESS_KEY=seaweedpass \
  -e AWS_REGION=us-east-1 \
  -e AWS_ALLOW_HTTP=true \
  --entrypoint python3 \
  ghcr.io/bzkf/fhir-to-lakehouse:v1.16.0 \
  src/lakehousekeeper.py vacuum --bucket-name=fhir --database-name-prefix=warehouse/ --dry-run=true
```

<!-- x-release-please-end-version -->

## Development

Install `uv` <https://docs.astral.sh/uv/getting-started/installation/#installation-methods>:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Running Spark locally requires Java 21 (see [.java-version](.java-version)). Newer versions are not yet supported
by Spark 4.0.

Install dependencies using

```sh
uv sync
```

The [compose.yaml](compose.yaml) contains the development fixtures required to run the program out-of-the-box:

- Apache Kafka (Exposed on <127.0.0.1:9094>)
- AKHQ - an Apache Kafka UI (Exposed on <127.0.0.1:8084>)
- SeaweedFS (Exposed on 127.0.0.1:8333 for the UI)
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
