import os
import sys
import time

import typed_settings as ts
from delta import DeltaTable
from loguru import logger
from pathling import PathlingContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import get_meter_provider, set_meter_provider, Histogram
from opentelemetry.sdk.metrics import MeterProvider

from prometheus_client import start_http_server

HERE = os.path.abspath(os.path.dirname(__file__))


class MeasureElapsed:
    def __init__(self, histogram: Histogram, attributes: dict[str, str]):
        self.histogram = histogram
        self.attributes = attributes

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.time = time.perf_counter() - self.start
        self.histogram.record(self.time, self.attributes)
        logger.info("Elapsed time: {time}", time=self.time)


@ts.settings
class KafkaSettings:
    bootstrap_servers: str = "localhost:9094"
    topics: str = "fhir.msg"
    max_offsets_per_trigger: int = 10000
    min_offsets_per_trigger: int = 10000
    max_trigger_delay: str = "15m"


@ts.settings
class SparkSettings:
    install_packages_and_exit: bool = False
    master: str = "local[*]"
    s3_endpoint: str = "localhost:9000"
    s3_connection_ssl_enabled: str = "false"
    warehouse_dir: str = os.path.join(HERE, "warehouse")
    checkpoint_dir: str = "s3a://fhir/checkpoint"
    driver_memory: str = "4g"
    upkeep_interval: int = 50
    streaming_processing_time: str = "0 seconds"


@ts.settings
class Settings:
    kafka: KafkaSettings
    spark: SparkSettings
    aws_access_key_id: str = "admin"
    aws_secret_access_key: str = ts.secret(default="miniopass")
    delta_database_dir: str = "s3a://fhir/warehouse"
    vacuum_retention_hours: int = 24
    metrics_port: int = 8000
    metrics_addr: str = "0.0.0.0"


settings = ts.load(Settings, appname="fhir_to_lakehouse", env_prefix="")

logger.info("Settings: {settings}", settings=settings)

start_http_server(port=settings.metrics_port, addr=settings.metrics_addr)

reader = PrometheusMetricReader()
# Meter is responsible for creating and recording metrics
set_meter_provider(MeterProvider(metric_readers=[reader]))
meter = get_meter_provider().get_meter("fhir_to_lakehouse.instrumentation")

delta_operations_timer = meter.create_histogram(
    name="delta-operation-duration",
    unit="seconds",
    description="Duration of Delta Table operations",
)

resources_processed_counter = meter.create_counter(
    name="resources-processed-total",
    unit="{Count}",
    description="Total number of resources written or deleted from Delta Tables",
)

# other config can be set via $SPARK_HOME/conf/spark-defaults.conf,
# e.g. compression type.
spark = (
    SparkSession.builder.master(settings.spark.master)
    .appName("fhir_to_lakehouse")
    .config(
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
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.driver.memory",
        settings.spark.driver_memory,
    )
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.ui.prometheus.enabled", "true")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", settings.spark.warehouse_dir)
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true",
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        settings.spark.s3_endpoint,
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        settings.spark.s3_connection_ssl_enabled,
    )
    .config("fs.s3a.access.key", settings.aws_access_key_id)
    .config("fs.s3a.secret.key", settings.aws_secret_access_key)
    .getOrCreate()
)

if settings.spark.install_packages_and_exit:
    logger.info("Exiting after installing packages")
    sys.exit()

pc = PathlingContext.create(spark)


df = (
    pc.spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
    .option("subscribe", settings.kafka.topics)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .option("groupIdPrefix", "fhir-to-lakehouse")
    .option("includeHeaders", "true")
    .option("maxOffsetsPerTrigger", str(settings.kafka.max_offsets_per_trigger))
    .option("minOffsetsPerTrigger", str(settings.kafka.min_offsets_per_trigger))
    .option("maxTriggerDelay", settings.kafka.max_trigger_delay)
    .load()
)

fhir_bundle_schema = StructType(
    [
        StructField(
            "entry",
            ArrayType(
                StructType(
                    [
                        StructField("resource", StringType(), True),
                        StructField(
                            "request",
                            StructType(
                                [
                                    StructField("method", StringType(), True),
                                    StructField("url", StringType(), True),
                                ]
                            ),
                        ),
                    ]
                )
            ),
            True,
        ),
    ]
)


def process_batch(micro_batch_df: DataFrame, batch_id: int):
    # might not be super efficient to log the batch size
    logger.info(
        "Processing batch {batch_id} containing {batch_size} rows",
        batch_id=batch_id,
        batch_size=micro_batch_df.count(),
    )

    micro_batch_df = micro_batch_df.withColumn(
        "bundle", micro_batch_df.value.cast("string")
    )

    parsed = micro_batch_df.withColumn(
        "parsed_bundle", F.from_json("bundle", fhir_bundle_schema)
    )

    df_exploded = parsed.withColumn(
        "entry",
        F.explode(F.col("parsed_bundle.entry")),
    )

    df_result = (
        df_exploded.withColumn("resource", F.col("entry.resource"))
        .withColumn("request_method", F.col("entry.request.method"))
        .withColumn("request_url", F.col("entry.request.url"))
        .withColumn("request_url_split", F.split("entry.request.url", "/"))
    )

    df_result = df_result.withColumn(
        "resource_type", df_result["request_url_split"].getItem(0)
    ).withColumn("request_resource_id", df_result["request_url_split"].getItem(1))

    resource_types_in_batch = [
        row["resource_type"]
        for row in df_result.select("resource_type").distinct().collect()
    ]

    logger.info(
        "Resource types in batch: {resource_types_in_batch}",
        resource_types_in_batch=resource_types_in_batch,
    )

    # TODO: find a way to run this in parallel per resource type
    #       - order, then partition by resource type (2 consecutive foreachBatch)
    for resource_type in resource_types_in_batch:
        # TODO: double-check if the sorting here is correct

        put_df = (
            df_result.filter(
                f"resource_type = '{resource_type}' and request_method = 'PUT'"
            )
            .sort(["timestamp", "partition", "offset"], ascending=False)
            .drop_duplicates(["request_url"])
        )

        resource_df = pc.encode(
            put_df,
            resource_type,
            column="resource",
        )

        resource_delta_table_path = os.path.join(
            settings.delta_database_dir, f"{resource_type}.parquet"
        )

        delta_table = (
            DeltaTable.createIfNotExists(spark)
            .tableName(resource_type)
            .location(resource_delta_table_path)
            .addColumns(resource_df.schema)
            .execute()
        )

        logger.info(
            "Table details: {details}", details=delta_table.detail().toJSON().collect()
        )

        with MeasureElapsed(
            delta_operations_timer,
            {"operation": "merge", "resource_type": resource_type},
        ):
            merge_into_table(resource_df, resource_type, delta_table)

        delete_df = (
            df_result.filter(
                f"resource_type = '{resource_type}' and request_method = 'DELETE'"
            )
            .sort(["timestamp", "partition", "offset"], ascending=False)
            .drop_duplicates(["request_url"])
        )

        with MeasureElapsed(
            delta_operations_timer,
            {"operation": "delete", "resource_type": resource_type},
        ):
            delete_from_table(delete_df, resource_type, delta_table)

        # TODO: should vacuum all tables, not just the ones in the batch
        if batch_id % settings.spark.upkeep_interval == 0:
            optimize_and_vacuum_table(delta_table, resource_type=resource_type)

        logger.info("Finished processing batch {batch_id}", batch_id=batch_id)


def merge_into_table(
    resource_df: DataFrame, resource_type: str, delta_table: DeltaTable
):
    resources_count = resource_df.count()

    logger.info(
        "Merging into table {resource_type} with {resources_count} rows",
        resource_type=resource_type,
        resources_count=resources_count,
    )

    (
        delta_table.alias("t")
        .merge(resource_df.alias("s"), "s.id = t.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    resources_processed_counter.add(
        resources_count, {"operation": "written", "resource_type": resource_type}
    )


def delete_from_table(
    delete_df: DataFrame,
    resource_type: str,
    delta_table: DeltaTable,
):
    deletes_count = delete_df.count()

    logger.info(
        "Deleting from table {resource_type} with {delete_df_size} rows",
        resource_type=resource_type,
        delete_df_size=deletes_count,
    )

    (
        delta_table.alias("t")
        .merge(delete_df.alias("s"), "s.request_resource_id = t.id")
        .whenMatchedDelete()
        .execute()
    )

    resources_processed_counter.add(
        deletes_count, {"operation": "delete", "resource_type": resource_type}
    )


def optimize_and_vacuum_table(delta_table: DeltaTable, resource_type: str):
    logger.info("Optimizing and vacuuming table")

    start = time.perf_counter()
    optimize_df = delta_table.optimize().executeCompaction()
    end = time.perf_counter()
    delta_operations_timer.record(
        end - start, {"operation": "optimize", "resource_type": resource_type}
    )

    logger.info(
        "Finished optimizing table. Statistics: {stats}",
        stats=optimize_df.toJSON().collect(),
    )

    start = time.perf_counter()
    delta_table.vacuum(retentionHours=settings.vacuum_retention_hours)
    end = time.perf_counter()
    delta_operations_timer.record(
        end - start, {"operation": "vacuum", "resource_type": resource_type}
    )
    logger.info("Finished vacuuming table")


# Write the output of a streaming aggregation query into Delta table
df.writeStream.option("checkpointLocation", settings.spark.checkpoint_dir).foreachBatch(
    process_batch
).outputMode("update").queryName("fhir_bundles_to_delta_tables").trigger(
    processingTime=settings.spark.streaming_processing_time
).start()

spark.streams.awaitAnyTermination()
