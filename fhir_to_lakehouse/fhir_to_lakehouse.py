import os
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

from pathling import PathlingContext


HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(HERE, "data")
NDJSON_DIR = os.path.join(DATA_DIR, "resources")
WAREHOUSE_DIR = os.path.join(HERE, "warehouse")

# TODO: move both to s3
CHECKPOINT_DIR = os.path.join(HERE, "checkpoints")
DELTA_DIR = os.path.join(HERE, "delta")

spark = (
    SparkSession.builder.config(
        "spark.jars.packages",
        "au.csiro.pathling:library-runtime:7.0.1,io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    .getOrCreate()
)

pc = PathlingContext.create(spark)


df = (
    pc.spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("subscribe", "fhir.msg")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .option("groupIdPrefix", "fhir-to-delta")
    .option("includeHeaders", "true")
    .option("maxOffsetsPerTrigger", "10000")
    .load()
)


def upsert_to_delta(micro_batch_df: DataFrame, batch_id):
    schema = StructType(
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

    micro_batch_df = micro_batch_df.withColumn(
        "bundle", micro_batch_df.value.cast("string")
    )

    parsed = micro_batch_df.withColumn("parsed_bundle", F.from_json("bundle", schema))

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

    resource_types_in_batch = df_result.select("resource_type").distinct().collect()

    for resource_type_row in resource_types_in_batch:
        resource_type = resource_type_row["resource_type"]

        # TODO: make sure the latest version of the resource is kept, not just any.
        #       order by timestamp/offset ?
        put_df = df_result.filter(
            f"resource_type = '{resource_type}' and request_method = 'PUT'"
        ).drop_duplicates(["request_url"])

        resource_df = pc.encode(
            put_df,
            resource_type,
            column="resource",
        )

        delta_table = (
            DeltaTable.createIfNotExists(spark)
            .tableName(resource_type)
            .location(os.path.join(DELTA_DIR, resource_type))
            .addColumns(resource_df.schema)
            .execute()
        )

        (
            delta_table.alias("t")
            .merge(resource_df.alias("s"), "s.id = t.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        delete_df = df_result.filter(
            f"resource_type = '{resource_type}' and request_method = 'DELETE'"
        ).drop_duplicates(["request_url"])

        (
            delta_table.alias("t")
            .merge(delete_df.alias("s"), "s.request_resource_id = t.id")
            .whenMatchedDelete()
            .execute()
        )

    # TODO: after upserting we could regularly run optimize and vacuum on the delta tables


# Write the output of a streaming aggregation query into Delta table
df.writeStream.option("checkpointLocation", CHECKPOINT_DIR).foreachBatch(
    upsert_to_delta
).trigger(processingTime="30 seconds").outputMode("update").start()

spark.streams.awaitAnyTermination()
