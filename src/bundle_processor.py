import json
import logging
import os

from delta import DeltaTable
from loguru import logger
from pathling import PathlingContext
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_attempt,
    wait_exponential,
)

from metrics import DURATION_SECONDS_BUCKETS, MeasureElapsed, meter
from settings import Settings

delta_operations_timer = meter.create_histogram(
    name="delta-operation-duration",
    unit="seconds",
    description="Duration of Delta Table operations",
    explicit_bucket_boundaries_advisory=DURATION_SECONDS_BUCKETS,
)

resources_processed_counter = meter.create_counter(
    name="resources-processed-total",
    unit="{Count}",
    description="Total number of resources written or deleted from Delta Tables",
)

delta_merge_rows_counter = meter.create_counter(
    name="delta-merge-rows",
    unit="{Count}",
    description="Rows inserted, updated, or deleted in Delta Tables by MERGE, "
    + "as reported by Delta",
)

# added to the MERGE source to tell upserts and deletes apart. Not part of the
# target table schema, so it is ignored by `whenMatchedUpdateAll` and
# `whenNotMatchedInsertAll`.
MERGE_OPERATION_COLUMN = "__fhir_to_lakehouse_request_method"


class BundleProcessor:
    def __init__(self, pc: PathlingContext, settings: Settings):
        self.pc = pc
        self.settings = settings

    def prepare_stream(self, df: DataFrame):
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

        df_result = (
            df.withColumn("bundle", F.col("value").cast("string"))
            .withColumn("parsed_bundle", F.from_json("bundle", fhir_bundle_schema))
            .withColumn("entry", F.explode("parsed_bundle.entry"))
            .withColumn("resource", F.col("entry.resource"))
            .withColumn("request_method", F.col("entry.request.method"))
            .withColumn("request_url", F.col("entry.request.url"))
            .withColumn("request_url_split", F.split("request_url", "/"))
            .withColumn("resource_type", F.col("request_url_split").getItem(0))
            .withColumn("request_resource_id", F.col("request_url_split").getItem(1))
            # each exploded row would otherwise carry up to three copies of the
            # entire bundle (value, bundle, parsed_bundle). Catalyst usually prunes
            # them, but not once the DataFrame is persisted in `process_batch`.
            .drop(
                "key",
                "value",
                "headers",
                "bundle",
                "parsed_bundle",
                "entry",
                "request_url_split",
            )
        )

        return df_result

    def process_batch(
        self, micro_batch_df: DataFrame, batch_id: int, query_name: str = "default"
    ):
        with logger.contextualize(batch_id=batch_id, query_name=query_name):
            logger.info(
                "Processing batch {batch_id}",
                batch_id=batch_id,
            )

            if micro_batch_df.isEmpty():
                logger.info("Batch is empty, skipping")
                return

            # see also <https://stackoverflow.com/a/54738843>
            # Window to get the latest message per request_url
            # whether the partition is sorted asc or desc isn't really relevant.
            # The request_url contains the resource type, so this is the same as
            # deduplicating per resource type.
            window = Window.partitionBy("request_url").orderBy(
                F.col("partition").asc(), F.col("offset").desc()
            )

            # only returns the latest (first) row entry per request_url
            # so if there's both a DELETE and a PUT in the batch and the DELETE
            # is ordered after the PUT, then only the DELETE is returned.
            # Persisted so the micro-batch is only read from Kafka and parsed once,
            # instead of once for every action below.
            # Without the coalesce, the cached DataFrame keeps the window's
            # `spark.sql.shuffle.partitions` (200 by default) partitions, which
            # results in thousands of tiny tasks for every batch.
            latest_df = (
                micro_batch_df.withColumn("row_num", F.row_number().over(window))
                .filter(F.col("row_num") == 1)
                .drop("row_num")
                .coalesce(self.pc.spark.sparkContext.defaultParallelism)
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            try:
                self._process_latest_entries(latest_df, batch_id)
            finally:
                latest_df.unpersist()

    def _process_latest_entries(self, latest_df: DataFrame, batch_id: int):
        # a single job for all counts instead of separate count(), distinct()
        # and per-method counts
        counts_by_resource_type: dict[str, dict[str, int]] = {}
        for row in (
            latest_df.groupBy("resource_type", "request_method").count().collect()
        ):
            counts_by_resource_type.setdefault(row["resource_type"], {})[
                row["request_method"]
            ] = row["count"]

        batch_size = sum(
            count
            for counts in counts_by_resource_type.values()
            for count in counts.values()
        )

        with logger.contextualize(batch_size=batch_size):
            logger.info(
                "Resource types in batch: {resource_types_in_batch}",
                resource_types_in_batch=counts_by_resource_type,
            )

            # for all types set in `resource_types_to_process_in_parallel`, there
            # should only ever be one resource_type in the batch.
            # In the default case, the batch may contain multiple resource types.
            for resource_type, counts in counts_by_resource_type.items():
                only_latest_df = latest_df.filter(
                    F.col("resource_type") == resource_type
                )

                if self.settings.log_resource_count_by_source_topic:
                    topic_counts = only_latest_df.groupBy("topic").count()
                    # topic_counts.toJSON() turns each row in the DF into a JSON
                    # string as a single column
                    # .collect() turns this into a list of JSON strings
                    # json.loads converts each JSON string into a Python dict
                    # so we can log it nicely
                    # The only difference is that the structured log part is a
                    # list of dicts instead of a single string
                    # 'topic_counts': [{'topic': 'fhir.msg', 'count': 47175}]
                    # instead of
                    # 'topic_counts': ['{"topic": "fhir.msg", "count": 47175}']
                    logger.info(
                        "Batch resource counts by source topic: {topic_counts}",
                        topic_counts=list(
                            map(json.loads, topic_counts.toJSON().collect())
                        ),
                    )

                with logger.contextualize(resource_type=resource_type):
                    self._process_df_of_single_resource_type(
                        only_latest_df,
                        resource_type,
                        batch_id,
                        put_count=counts.get("PUT", 0),
                        delete_count=counts.get("DELETE", 0),
                    )
                    logger.info("Finished processing resource batch")

            logger.info("Finished processing entire batch")

    def _process_df_of_single_resource_type(
        self,
        single_resource_type_df: DataFrame,
        resource_type: str,
        batch_id: int,
        put_count: int,
        delete_count: int,
    ):
        put_df = single_resource_type_df.filter(F.col("request_method") == "PUT")

        resource_df = self.pc.encode(
            put_df,
            resource_type,
            column="resource",
        )

        resource_delta_table_path = os.path.join(
            self.settings.delta_database_dir, f"{resource_type}.parquet"
        )

        delta_table_builder = (
            DeltaTable.createIfNotExists(self.pc.spark)
            .location(resource_delta_table_path)
            .addColumns(resource_df.schema)
            .property(
                "delta.autoOptimize.autoCompact",
                self.settings.delta.auto_optimize_auto_compact,
            )
            .property(
                "delta.autoOptimize.optimizeWrite",
                self.settings.delta.auto_optimize_optimize_write,
            )
            .property(
                "delta.checkpointInterval",
                self.settings.delta.checkpoint_interval,
            )
            .property(
                "delta.checkpoint.writeStatsAsJson",
                self.settings.delta.checkpoint_write_stats_as_json,
            )
            .property(
                "delta.checkpoint.writeStatsAsStruct",
                self.settings.delta.checkpoint_write_stats_as_struct,
            )
        )

        clustering_columns_by_resource_type = (
            self.settings.delta.clustering_columns_by_resource_type
        )
        if cluster_columns := clustering_columns_by_resource_type.get(resource_type):
            delta_table_builder = delta_table_builder.clusterBy(cluster_columns)

        delta_table = delta_table_builder.execute()

        logger.info(
            "Table details: {details}",
            details=delta_table.detail().toJSON().collect(),
        )

        # XXX: not necessary for every batch...
        if self.settings.metastore_url:
            with MeasureElapsed(
                delta_operations_timer,
                {"operation": "register", "resource_type": resource_type},
            ):
                self._register_table_in_metastore(
                    delta_table, resource_delta_table_path
                )

        # upserts and deletes are applied in a single MERGE, so a batch results in
        # one commit per table instead of two
        merge_source_df = resource_df.withColumn(MERGE_OPERATION_COLUMN, F.lit("PUT"))

        if delete_count > 0:
            delete_df = single_resource_type_df.filter(
                F.col("request_method") == "DELETE"
            ).select(
                F.col("request_resource_id").alias("id"),
                F.lit("DELETE").alias(MERGE_OPERATION_COLUMN),
            )
            merge_source_df = merge_source_df.unionByName(
                delete_df, allowMissingColumns=True
            )

        with MeasureElapsed(
            delta_operations_timer,
            {"operation": "merge", "resource_type": resource_type},
        ):
            self._merge_into_table(
                merge_source_df, resource_type, delta_table, put_count, delete_count
            )

        # TODO: should vacuum all tables, not just the ones in the batch
        if batch_id % self.settings.spark.upkeep_interval == 0:
            self._optimize_and_vacuum_table(delta_table, resource_type=resource_type)

    @retry(
        wait=wait_exponential(multiplier=1, min=5, max=30),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _merge_into_table(
        self,
        merge_source_df: DataFrame,
        resource_type: str,
        delta_table: DeltaTable,
        put_count: int,
        delete_count: int,
    ):
        logger.info(
            "Merging into table {resource_type} with {put_count} upserts "
            + "and {delete_count} deletes",
            resource_type=resource_type,
            put_count=put_count,
            delete_count=delete_count,
        )

        merge_metrics_df = (
            delta_table.alias("t")
            .merge(merge_source_df.alias("s"), "s.id = t.id")
            .whenMatchedDelete(f"s.{MERGE_OPERATION_COLUMN} = 'DELETE'")
            .whenMatchedUpdateAll(f"s.{MERGE_OPERATION_COLUMN} = 'PUT'")
            .whenNotMatchedInsertAll(f"s.{MERGE_OPERATION_COLUMN} = 'PUT'")
            .execute()
        )

        merge_metrics = [row.asDict() for row in merge_metrics_df.collect()]

        logger.info("Merge metrics: {merge_metrics}", merge_metrics=merge_metrics)

        for row in merge_metrics:
            for action in ("inserted", "updated", "deleted"):
                delta_merge_rows_counter.add(
                    row[f"num_{action}_rows"],
                    {"action": action, "resource_type": resource_type},
                )

        resources_processed_counter.add(
            put_count, {"operation": "written", "resource_type": resource_type}
        )
        resources_processed_counter.add(
            delete_count, {"operation": "delete", "resource_type": resource_type}
        )

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _optimize_and_vacuum_table(self, delta_table: DeltaTable, resource_type: str):
        logger.info("Optimizing and vacuuming table")

        with MeasureElapsed(
            delta_operations_timer,
            {"operation": "optimize", "resource_type": resource_type},
        ):
            optimize_df = delta_table.optimize().executeCompaction()

        logger.info(
            "Finished optimizing table. Statistics: {stats}",
            stats=optimize_df.toJSON().collect(),
        )

        with MeasureElapsed(
            delta_operations_timer,
            {"operation": "vacuum", "resource_type": resource_type},
        ):
            delta_table.vacuum(retentionHours=self.settings.vacuum_retention_hours)

        logger.info("Finished vacuuming table.")

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _register_table_in_metastore(self, table: DeltaTable, table_path: str):
        logger.info(
            "Registering '{table}' in '{metastore}'",
            table=table_path,
            metastore=self.settings.metastore_url,
        )

        # the second to last part when splitting by '/' is 'default'
        schema = table_path.split("/")[-2]

        # the table path but without the table name
        schema_path = table_path.removesuffix(table_path.split("/")[-1])

        # the final folder name without the '.parquet' extension
        table_name = table_path.split("/")[-1].removesuffix(".parquet")

        create_schema_query = (
            f"CREATE SCHEMA IF NOT EXISTS {schema} LOCATION '{schema_path}'"
        )
        logger.info(create_schema_query)
        self.pc.spark.sql(create_schema_query)

        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} "
            + f"USING DELTA LOCATION '{table_path}'"
        )
        logger.info(create_table_query)
        self.pc.spark.sql(create_table_query)
