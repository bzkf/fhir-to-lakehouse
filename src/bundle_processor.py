import logging
import os

from delta import DeltaTable
from loguru import logger
from pathling import PathlingContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_attempt,
    wait_exponential,
)

from metrics import MeasureElapsed, meter
from settings import Settings

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
        )

        return df_result

    def process_batch(self, micro_batch_df: DataFrame, batch_id: int, query_name: str = "default"):
        with logger.contextualize(batch_id=batch_id, query_name=query_name):
            # might not be super efficient to log the batch size
            logger.info(
                "Processing batch {batch_id} containing {batch_size} rows",
                batch_id=batch_id,
                batch_size=micro_batch_df.count(),
            )

            if micro_batch_df.isEmpty():
                logger.info("Batch is empty, skipping")
                return

            resource_types_in_batch = [
                row["resource_type"]
                for row in micro_batch_df.select("resource_type").distinct().collect()
            ]

            logger.info(
                "Resource types in batch: {resource_types_in_batch}",
                resource_types_in_batch=resource_types_in_batch,
            )

            # for all types set in `resource_types_to_process_in_parallel`, there
            # should only ever be one resource_type in the batch.
            # In the default case, the batch may contain multiple resource types.
            for resource_type in resource_types_in_batch:
                resource_df = micro_batch_df.filter(
                    f"resource_type = '{resource_type}'"
                )

                # see also <https://stackoverflow.com/a/54738843>
                # Window to get the latest message per request_url
                # whether the partition is sorted asc or desc isn't really relevant
                window = Window.partitionBy("request_url").orderBy(
                    F.col("partition").asc(), F.col("offset").desc()
                )

                # only returns the latest (first) row entry per request_url
                # so if there's both a DELETE and a PUT in the batch and the DELETE
                # is ordered after the PUT, then only the DELETE is returned
                only_latest_df = (
                    resource_df.withColumn("row_num", F.row_number().over(window))
                    .filter(F.col("row_num") == 1)
                    .drop("row_num")
                )

                with logger.contextualize(resource_type=resource_type):
                    self._process_df_of_single_resource_type(
                        only_latest_df, resource_type, batch_id
                    )
                    logger.info("Finished processing resource batch")

            logger.info("Finished processing entire batch")

    def _process_df_of_single_resource_type(
        self, single_resource_type_df: DataFrame, resource_type: str, batch_id: int
    ):
        put_df = single_resource_type_df.filter("request_method = 'PUT'")

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

        with MeasureElapsed(
            delta_operations_timer,
            {"operation": "merge", "resource_type": resource_type},
        ):
            self._merge_into_table(resource_df, resource_type, delta_table)

        delete_df = single_resource_type_df.filter("request_method = 'DELETE'")

        if delete_df.count() > 0:
            with MeasureElapsed(
                delta_operations_timer,
                {"operation": "delete", "resource_type": resource_type},
            ):
                self._delete_from_table(delete_df, resource_type, delta_table)

        # TODO: should vacuum all tables, not just the ones in the batch
        if batch_id % self.settings.spark.upkeep_interval == 0:
            self._optimize_and_vacuum_table(delta_table, resource_type=resource_type)

    @retry(
        wait=wait_exponential(multiplier=1, min=5, max=30),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _merge_into_table(
        self, resource_df: DataFrame, resource_type: str, delta_table: DeltaTable
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

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _delete_from_table(
        self,
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
