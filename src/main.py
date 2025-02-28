import sys

from loguru import logger
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from pathling import PathlingContext
from prometheus_client import start_http_server
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bundle_processor import BundleProcessor
from settings import settings

logger.info("Settings: {settings}", settings=settings)

start_http_server(port=settings.metrics_port, addr=settings.metrics_addr)

reader = PrometheusMetricReader()
# Meter is responsible for creating and recording metrics
set_meter_provider(MeterProvider(metric_readers=[reader]))


# other config can be set via $SPARK_HOME/conf/spark-defaults.conf,
# e.g. compression type.
spark_config = (
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
    .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
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
    .config("fs.s3a.committer.name", "magic")
    .config("fs.s3a.committer.magic.enabled", "true")
    .config("fs.s3a.access.key", settings.aws_access_key_id)
    .config("fs.s3a.secret.key", settings.aws_secret_access_key)
)

if settings.metastore_url:
    spark_config.config("spark.hive.metastore.uris", settings.metastore_url).config(
        "spark.sql.catalogImplementation", "hive"
    )

spark = spark_config.getOrCreate()

if settings.spark.install_packages_and_exit:
    logger.info("Exiting after installing packages")
    sys.exit()

pc = PathlingContext.create(
    spark,
    enable_extensions=True,
    enable_delta=True,
    enable_terminology=False,
    terminology_server_url="http://localhost/not-a-real-server",
)


processor = BundleProcessor(pc, settings)


reader = (
    pc.spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
    .option("subscribe", settings.kafka.topics)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", str(settings.kafka.fail_on_data_loss).lower())
    .option("groupIdPrefix", "fhir-to-lakehouse")
    .option("includeHeaders", "true")
    .option("maxOffsetsPerTrigger", str(settings.kafka.max_offsets_per_trigger))
    .option("minOffsetsPerTrigger", str(settings.kafka.min_offsets_per_trigger))
    .option("maxTriggerDelay", settings.kafka.max_trigger_delay)
)

if settings.kafka.security_protocol == "SSL":
    reader = (
        reader.option("kafka.security.protocol", "SSL")
        .option("kafka.ssl.truststore.type", settings.kafka.ssl.truststore_type)
        .option(
            "kafka.ssl.truststore.location", settings.kafka.ssl.trust_store_location
        )
        .option(
            "kafka.ssl.truststore.password", settings.kafka.ssl.trust_store_password
        )
        .option("kafka.ssl.keystore.type", settings.kafka.ssl.keystore_type)
        .option("kafka.ssl.keystore.location", settings.kafka.ssl.key_store_location)
        .option("kafka.ssl.keystore.password", settings.kafka.ssl.key_store_password)
    )

df = reader.load()

df_result = processor.prepare_stream(df)

for resource_type in settings.resource_types_to_process_in_parallel:
    filtered_df = df_result.filter(F.col("resource_type") == resource_type)

    (
        filtered_df.writeStream.outputMode(settings.spark.output_mode)
        .option(
            "checkpointLocation", settings.spark.checkpoint_dir + f"/{resource_type}"
        )
        .queryName(f"process_{resource_type}")
        .foreachBatch(processor.process_batch)
        .trigger(processingTime=settings.spark.streaming_processing_time)
        .start()
    )

default_df = df_result.filter(
    ~F.col("resource_type").isin(settings.resource_types_to_process_in_parallel)
)

(
    default_df.writeStream.outputMode(settings.spark.output_mode)
    .option("checkpointLocation", settings.spark.checkpoint_dir + "/default")
    .queryName("process_default")
    .foreachBatch(processor.process_batch)
    .trigger(processingTime=settings.spark.streaming_processing_time)
    .start()
)

spark.streams.awaitAnyTermination()
