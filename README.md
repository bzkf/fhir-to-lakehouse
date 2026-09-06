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

[fhir-to-lakehouse.delta.tables.Patient]
clustering_columns = ["id", "birthDate"]

[fhir-to-lakehouse.delta.tables.Observation]
clustering_columns = ["id", "effectiveDateTime", "subject"]

[fhir-to-lakehouse.delta.tables.Condition]
clustering_columns = ["id", "recordedDate", "onsetDateTime", "subject"]
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
tables in a Hive metastore instead, or `iceberg.catalog_type = "rest"` together with `iceberg.catalog_uri` and
`iceberg.catalog_warehouse` to register tables in an Iceberg REST catalog such as
[Lakekeeper](https://docs.lakekeeper.io/) instead (see "Iceberg REST catalog (Lakekeeper)" below). Table
maintenance (file compaction via `rewrite_data_files` and old-snapshot expiry via `expire_snapshots`) runs
periodically, just like `OPTIMIZE`/`VACUUM` do for Delta tables.

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
typically a content hash/UUID with no natural range locality), also configure hash-bucket partitioning and a sort
order per resource type under `iceberg.tables.<ResourceType>`:

```toml
[fhir-to-lakehouse.iceberg.tables.Observation]
bucket_column = "id"
bucket_count = 128
sort_columns = ["id"]
```

Bucketing on `id` deterministically confines every row to exactly one of N partitions, so a MERGE only ever has
to consider the partitions its batch's ids hash into — this holds regardless of how well the rest of the table
happens to be clustered, which matters for a hash-like key where a plain sort order alone doesn't create range
locality. The sort order is applied locally per write task on every batch, and fully enforced across files during
periodic maintenance (which switches to `rewrite_data_files(..., strategy => 'sort')` automatically once
`sort_columns` is set for that resource type's table settings).

There's no universal bucket count — size it so each bucket ends up around one-to-a-few `write.target-file-size-bytes`
files once the table matures: `bucket_count ≈ expected_total_data_size / (write.target_file_size_bytes × files_per_bucket)`.
For "several hundred million" `Observation` rows, a starting point in the range of 64–256 (pick a power of 2) is
reasonable; check the average file size after a few compaction passes and adjust if files consistently end up far
from the target size.

#### Iceberg REST catalog (Lakekeeper)

[compose.lakekeeper.yaml](compose.lakekeeper.yaml) adds a [Lakekeeper](https://docs.lakekeeper.io/) REST catalog
(plus its Postgres metadata store) to the dev fixtures, for testing `iceberg.catalog_type = "rest"` against
something closer to a production catalog than the default Hadoop catalog. It runs without authentication, matching
this project's other dev fixtures (Kafka, MinIO) - don't reuse it as-is for anything but local dev.

Start it together with the base fixtures ([compose.yaml](compose.yaml), for Kafka/MinIO):

```sh
docker compose -f compose.yaml -f compose.lakekeeper.yaml up
```

This also runs two one-shot containers that call Lakekeeper's management API to bootstrap it and register a
warehouse named `fhir` backed by the same MinIO bucket the Hadoop catalog uses
(storage config in [hack/lakekeeper/create-fhir-warehouse.json](hack/lakekeeper/create-fhir-warehouse.json)).
Lakekeeper's REST API is then reachable at <http://localhost:8181>.

Point the application at it with a settings file:

```toml
[fhir-to-lakehouse]
table_format = "iceberg"

[fhir-to-lakehouse.iceberg]
catalog_type = "rest"
catalog_uri = "http://localhost:8181/catalog"
catalog_warehouse = "fhir"
```

```sh
FHIR_TO_LAKEHOUSE_SETTINGS=settings.toml uv run src/main.py
```

Once running (with `mock-data-loader` from compose.yaml feeding sample bundles through Kafka), tables get created
in the `fhir` warehouse's `default` namespace on first batch per resource type, the same way they would against the
Hadoop catalog. To confirm, either point a `spark-sql`/`pyspark` shell at the same `iceberg.catalog_type = "rest"`
config shown above and run `SHOW TABLES IN iceberg.default;`, or list them via Lakekeeper's REST API - the Iceberg
REST spec addresses warehouses by an opaque `prefix` (not the warehouse name), which the catalog resolves in its
`/v1/config` response:

```sh
prefix=$(curl -s "http://localhost:8181/catalog/v1/config?warehouse=fhir" | jq -r '.defaults.prefix')
curl -s "http://localhost:8181/catalog/v1/$prefix/namespaces/default/tables" | jq
```

### Spark Config

By default, the `SPARK_CONF_DIR` environment variable inside the container is set to `/app/spark/conf`, so you
can mount a `spark-defaults.conf` file at `/app/spark/conf/spark-defaults.conf` to override any Spark setting.

## Lakehousekeeper

A CLI tool called `lakehousekeeper` is also part of the container distribution.
It implements commands for vacuuming, optimizing, and registering Delta tables from S3-compatible object storage,
and for running maintenance (`rewrite_data_files`/`expire_snapshots`) against Iceberg tables via `iceberg-optimize`
and `iceberg-expire-snapshots`. Unlike the Delta commands, which discover tables by listing an S3 prefix directly,
the Iceberg commands go through the catalog (`--catalog-type hadoop`/`hive`/`rest`, matching `iceberg.catalog_type`)
and sweep every table in a given `--namespace`:

```sh
lakehousekeeper.py iceberg-optimize --catalog-type hadoop \
  --warehouse-dir s3a://fhir/warehouse-iceberg --namespace default

lakehousekeeper.py iceberg-expire-snapshots --catalog-type rest \
  --catalog-uri http://lakekeeper:8181/catalog --catalog-warehouse fhir --namespace default
```

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
