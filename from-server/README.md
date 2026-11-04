# analytics-on-fhir

A Docker Compose stack that generates synthetic patients, loads them into a FHIR server, and
incrementally exports them as Delta Lake tables to S3, registered in a Hive metastore and
queryable with SQL using Trino. The [agent](agent/README.md) folder adds an AI agent that answers
questions about the data and creates reports, and the [wrenai](wrenai/README.md) folder tests
WrenAI as a semantic layer for the tables.

`docker compose up` starts the long-running services:

| Service          | What it is                                                                  | URL                                                    |
| ---------------- | --------------------------------------------------------------------------- | ------------------------------------------------------ |
| `blaze`          | [Blaze](https://github.com/samply/blaze) FHIR server                        | <http://localhost:8080/fhir>                           |
| `seaweedfs`      | S3 object store with the `fhir` bucket, credentials `admin` / `seaweedpass` | <http://localhost:8333>, UI at <http://localhost:8888> |
| `hive-metastore` | Hive metastore, storing its catalog in the `metastore-db` Postgres database |                                                        |
| `trino`          | [Trino](https://trino.io) SQL query engine for the tables, read-only        | <http://localhost:8081>                                |

The steps of the pipeline are run one at a time, in two phases:

| Phase     | Step              | What it does                                                                                                                                                                                                                                                                                 |
| --------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Demo data | `synthea`         | Generates 100 patients as transaction bundles using [Synthea](https://github.com/synthetichealth/synthea)                                                                                                                                                                                    |
| Demo data | `upload`          | Uploads the bundles to Blaze using [blazectl](https://github.com/samply/blazectl)                                                                                                                                                                                                            |
| Usage     | `import`          | Downloads the resources changed since the last run from Blaze in batches of 1 million, and merges each batch into Delta Lake tables at `s3a://fhir/warehouse/<ResourceType>.parquet` before downloading the next one, using the [Pathling CLI](https://pathling.csiro.au/docs/libraries/cli) |
| Usage     | `lakehousekeeper` | Registers the tables in the Hive metastore as `warehouse.<ResourceType>`, then runs OPTIMIZE and VACUUM, using [lakehousekeeper](https://github.com/bzkf/fhir-to-lakehouse#lakehousekeeper)                                                                                                  |
| Usage     | `query`           | Runs the example query in [scripts/query.py](scripts/query.py) against the Delta Lake tables, using the Pathling CLI                                                                                                                                                                         |

All steps except `lakehousekeeper` use the [ig-build-tools](https://github.com/miracum/ig-build-tools)
image, which contains Synthea, blazectl, and the Pathling CLI. They run as its unprivileged
`nonroot` user.

## Usage

Start the long-running services:

```sh
docker compose up -d --wait
```

Then run the steps in sequence. The demo data phase generates synthetic patients and loads them
into Blaze, which takes about a minute:

```sh
docker compose run --rm synthea
docker compose run --rm upload
```

The usage phase exports the new and changed resources from Blaze into the lakehouse and queries
it, which takes about 3 minutes the first time:

```sh
docker compose run --rm import
docker compose run --rm lakehousekeeper
docker compose run --rm query
```

`query` prints the most common conditions by number of patients:

```text
+---------------------------------------+--------+
|condition                              |patients|
+---------------------------------------+--------+
|Medication review due (situation)      |108     |
|Full-time employment (finding)         |91      |
|Stress (finding)                       |89      |
|Gingivitis (disorder)                  |86      |
|Viral sinusitis (disorder)             |72      |
...
```

### Querying with SQL

Once `lakehousekeeper` has registered the tables, Trino can query them as
`delta.warehouse.<resourcetype>`, e.g. `delta.warehouse.condition`. For an interactive session,
run:

```sh
docker compose exec trino trino --catalog delta --schema warehouse
```

Or run a single query:

```sh
docker compose exec trino trino --output-format ALIGNED --execute "
  SELECT coding.display AS condition, count(DISTINCT c.subject.reference) AS patients
  FROM delta.warehouse.condition c
  CROSS JOIN UNNEST(c.code.coding) AS coding
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 5"
```

```text
             condition             | patients
-----------------------------------+----------
 Medication review due (situation) |      121
 Gingivitis (disorder)             |      100
 Stress (finding)                  |       99
 Full-time employment (finding)    |       97
 Viral sinusitis (disorder)        |       88
(5 rows)
```

The tables have one column per element of the resource, in lower case, with nested `row` and
`array` types, as encoded by Pathling. Trino reads the latest version of the tables with each
query, so changes from `import` are visible right away. Only new resource types need
`lakehousekeeper` to run again. Trino only allows reading, see
[access-control.properties](config/trino/access-control.properties).

The web UI at <http://localhost:8081> shows the running and recent queries. Any user name works
for logging in.

### Running the phases again

The usage phase only processes what changed since its last run: `import` searches Blaze for
`_lastUpdated=ge<start of the last download>`, and merges the results into the tables by
resource id. The start of the last download is stored as a checkpoint in S3, at
`s3://fhir/checkpoints/_last-download-checkpoint.json`:

```json
{
  "lastDownloadStartedAt": "2026-11-01T14:55:01Z"
}
```

To limit the disk space used, `import` downloads the resources in batches. It follows the
search's pages of 1000 resources, appending them to one NDJSON file per resource type, until the
batch has at least `BATCH_SIZE` resources, 1 million by default. It then merges the batch into
the tables, deletes the files, and continues with the next page. To use smaller batches, set
`BATCH_SIZE`:

```sh
BATCH_SIZE=100000 docker compose run --rm import
```

The checkpoint is only written once the last batch is merged. If `import` fails, running it again
starts over from the previous checkpoint, merging the batches that already succeeded again, which
doesn't change the tables.

Running the demo data phase again adds new patients to Blaze, so the next run of the usage phase
only downloads those. To generate fewer patients, set `SYNTHEA_POPULATION`:

```sh
SYNTHEA_POPULATION=10 docker compose run --rm synthea
docker compose run --rm upload
```

To start over, remove all containers and volumes, including the ones of the steps:

```sh
docker compose --profile steps down -v
```

### Running air-gapped

Apart from the images, nothing is downloaded at runtime. The ig-build-tools image comes with
Pathling's Spark packages (library runtime, Delta Lake) in its Ivy cache, so Spark resolves them
from there, and it includes the S3A filesystem.

Pull all images beforehand, including the ones of the steps:

```sh
docker compose --profile steps pull
```

To check that the stack works offline, add [compose.airgapped.yaml](compose.airgapped.yaml), which
runs it on an internal network without internet access. No ports are published to the host in
this mode. Setting `COMPOSE_FILE` applies it to all of the commands above:

```sh
export COMPOSE_FILE=compose.yaml:compose.airgapped.yaml
```

## Data layout

The steps exchange files using the `pipeline-data` volume:

```text
/data/synthea/fhir/           patient bundles of the last demo data phase
/data/synthea/providers/      organization and practitioner bundles, uploaded first
/data/ndjson/<Type>.ndjson    the batch of resources that import is downloading
```

The `fhir` bucket in S3 contains:

```text
warehouse/<Type>.parquet/                     Delta Lake table per resource type
checkpoints/_last-download-checkpoint.json    start time of the last download
```

## Notes

- Deleted resources aren't propagated, as a FHIR search doesn't return them.
- `SYNTHEA_POPULATION` is the number of living patients. Synthea also exports the patients
  who died during the simulation, so the default of 100 results in about 110 patients.
- Blaze's built-in `StructureDefinition` resources are downloaded, but Pathling doesn't
  encode them, so there's no table for them.
- VACUUM uses the default retention of 7 days, so it only deletes files that were removed
  from the tables more than a week ago.
- Blaze's paging links expire after 4 hours without a request, so merging a batch has to take
  less than that.
- `import` and `query` use `pathling run` instead of `pathling convert` and `pathling view`,
  because the CLI corrupts `s3a://` paths. See
  [pathling-cli-remote-paths-issue.md](pathling-cli-remote-paths-issue.md).
