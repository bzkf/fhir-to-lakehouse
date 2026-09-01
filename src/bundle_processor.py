import json
import logging
import os
import re
from datetime import UTC, datetime, timedelta

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

table_operations_timer = meter.create_histogram(
    name="table-operation-duration",
    unit="seconds",
    description="Duration of Delta/Iceberg Table operations",
    explicit_bucket_boundaries_advisory=DURATION_SECONDS_BUCKETS,
)

resources_processed_counter = meter.create_counter(
    name="resources-processed-total",
    unit="{Count}",
    description="Total number of resources written or deleted from Delta/Iceberg",
)

delta_merge_rows_counter = meter.create_counter(
    name="delta-merge-rows",
    unit="{Count}",
    description="Rows inserted, updated, deleted, or copied unchanged into new files "
    + "in Delta Tables by MERGE, as reported by Delta",
)

delta_merge_phase_timer = meter.create_histogram(
    name="delta-merge-phase-duration",
    unit="s",
    description="Duration of the phases of Delta MERGE operations, as reported by "
    + "Delta: materialize_source (reading and encoding the batch), scan (finding "
    + "the files with matching rows), and rewrite (writing the changed files)",
    explicit_bucket_boundaries_advisory=DURATION_SECONDS_BUCKETS,
)

delta_merge_files_counter = meter.create_counter(
    name="delta-merge-files",
    unit="{Count}",
    description="Files added to or removed from Delta Tables by MERGE",
)

delta_merge_bytes_counter = meter.create_counter(
    name="delta-merge-bytes",
    unit="By",
    description="Bytes added to or removed from Delta Tables by MERGE",
)

delta_table_size_gauge = meter.create_gauge(
    name="delta-table-size",
    unit="By",
    description="Size of the Delta Tables before the latest MERGE",
)

delta_table_files_gauge = meter.create_gauge(
    name="delta-table-files",
    unit="{Count}",
    description="Number of files of the Delta Tables before the latest MERGE",
)

# the phases reported in the history of a MERGE, by the name they are exported with
MERGE_PHASE_METRICS = {
    "materialize_source": "materializeSourceTimeMs",
    "scan": "scanTimeMs",
    "rewrite": "rewriteTimeMs",
}

# added to the MERGE source to tell upserts and deletes apart. Not part of the
# target table schema, so it is ignored by `whenMatchedUpdateAll` and
# `whenNotMatchedInsertAll`.
MERGE_OPERATION_COLUMN = "__fhir_to_lakehouse_request_method"

# FHIR resource type names are always PascalCase alphanumeric, e.g. "Patient".
# resource_type is derived from Kafka message content (the bundle entry's
# `request.url`), so it must be validated before it's interpolated into any
# SQL identifier (table/namespace names, CALL procedure arguments).
_VALID_RESOURCE_TYPE = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")


def _is_valid_resource_type(resource_type: str) -> bool:
    return bool(_VALID_RESOURCE_TYPE.fullmatch(resource_type))


class BundleProcessor:
    def __init__(self, pc: PathlingContext, settings: Settings):
        self.pc = pc
        self.settings = settings
        # table version of the latest MERGE whose history metrics were recorded,
        # by resource type
        self.last_recorded_merge_versions: dict[str, int] = {}

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
                if not _is_valid_resource_type(resource_type):
                    logger.warning(
                        "Skipping batch of invalid resource_type: {resource_type}",
                        resource_type=resource_type,
                    )
                    continue

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

        delete_df = single_resource_type_df.filter(F.col("request_method") == "DELETE")

        if self.settings.table_format == "iceberg":
            self._process_iceberg(
                resource_df,
                delete_df,
                resource_type,
                batch_id,
                put_count=put_count,
                delete_count=delete_count,
            )
        else:
            self._process_delta(
                resource_df,
                delete_df,
                resource_type,
                batch_id,
                put_count=put_count,
                delete_count=delete_count,
            )

    def _process_delta(
        self,
        resource_df: DataFrame,
        delete_df: DataFrame,
        resource_type: str,
        batch_id: int,
        put_count: int,
        delete_count: int,
    ):
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

        table_settings = self.settings.delta.tables.get(resource_type)
        if table_settings and table_settings.clustering_columns:
            delta_table_builder = delta_table_builder.clusterBy(
                table_settings.clustering_columns
            )
        if table_settings and table_settings.enable_deletion_vectors:
            delta_table_builder = delta_table_builder.property(
                "delta.enableDeletionVectors", "true"
            )

        delta_table = delta_table_builder.execute()

        table_details = delta_table.detail().first()
        if table_details is not None:
            logger.info("Table details: {details}", details=table_details.asDict())
            delta_table_size_gauge.set(
                table_details["sizeInBytes"], {"resource_type": resource_type}
            )
            delta_table_files_gauge.set(
                table_details["numFiles"], {"resource_type": resource_type}
            )

        # XXX: not necessary for every batch...
        if self.settings.metastore_url:
            with MeasureElapsed(
                table_operations_timer,
                {
                    "operation": "register",
                    "resource_type": resource_type,
                    "table_format": "delta",
                },
            ):
                self._register_table_in_metastore(
                    delta_table, resource_delta_table_path
                )

        # upserts and deletes are applied in a single MERGE, so a batch results in
        # one commit per table instead of two
        merge_source_df = resource_df.withColumn(MERGE_OPERATION_COLUMN, F.lit("PUT"))

        if delete_count > 0:
            delete_source_df = delete_df.select(
                F.col("request_resource_id").alias("id"),
                F.lit("DELETE").alias(MERGE_OPERATION_COLUMN),
            )
            merge_source_df = merge_source_df.unionByName(
                delete_source_df, allowMissingColumns=True
            )

        with MeasureElapsed(
            table_operations_timer,
            {
                "operation": "merge",
                "resource_type": resource_type,
                "table_format": "delta",
            },
        ):
            self._merge_into_table(
                merge_source_df, resource_type, delta_table, put_count, delete_count
            )

        # outside of the retried `_merge_into_table`, so a failure to read the
        # metrics never repeats the MERGE
        try:
            self._record_merge_history_metrics(delta_table, resource_type)
        except Exception as e:
            logger.warning("Failed to record the MERGE metrics: {e}", e=e)

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
            put_count,
            {
                "operation": "written",
                "resource_type": resource_type,
                "table_format": "delta",
            },
        )
        resources_processed_counter.add(
            delete_count,
            {
                "operation": "delete",
                "resource_type": resource_type,
                "table_format": "delta",
            },
        )

    def _record_merge_history_metrics(
        self, delta_table: DeltaTable, resource_type: str
    ):
        # the result of the MERGE only contains the row counts, everything else is
        # only part of the table history. Each table is written by a single query,
        # so the latest commit is the MERGE that just finished, unless it didn't
        # commit at all.
        commit = delta_table.history(1).first()
        if (
            commit is None
            or commit["operation"] != "MERGE"
            or self.last_recorded_merge_versions.get(resource_type) == commit["version"]
        ):
            return
        self.last_recorded_merge_versions[resource_type] = commit["version"]

        metrics = commit["operationMetrics"]
        attributes = {"resource_type": resource_type}

        for phase, key in MERGE_PHASE_METRICS.items():
            if key in metrics:
                delta_merge_phase_timer.record(
                    int(metrics[key]) / 1000, {**attributes, "phase": phase}
                )

        # rows that weren't changed, but had to be written again because they are
        # in the same file as a changed row
        delta_merge_rows_counter.add(
            int(metrics.get("numTargetRowsCopied", 0)),
            {**attributes, "action": "copied"},
        )

        for action in ("added", "removed"):
            delta_merge_files_counter.add(
                int(metrics.get(f"numTargetFiles{action.capitalize()}", 0)),
                {**attributes, "action": action},
            )
            delta_merge_bytes_counter.add(
                int(metrics.get(f"numTargetBytes{action.capitalize()}", 0)),
                {**attributes, "action": action},
            )

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _optimize_and_vacuum_table(self, delta_table: DeltaTable, resource_type: str):
        logger.info("Optimizing and vacuuming table")

        with MeasureElapsed(
            table_operations_timer,
            {
                "operation": "optimize",
                "resource_type": resource_type,
                "table_format": "delta",
            },
        ):
            optimize_df = delta_table.optimize().executeCompaction()

        logger.info(
            "Finished optimizing table. Statistics: {stats}",
            stats=optimize_df.toJSON().collect(),
        )

        with MeasureElapsed(
            table_operations_timer,
            {
                "operation": "vacuum",
                "resource_type": resource_type,
                "table_format": "delta",
            },
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

    def _process_iceberg(
        self,
        resource_df: DataFrame,
        delete_df: DataFrame,
        resource_type: str,
        batch_id: int,
        put_count: int,
        delete_count: int,
    ):
        # unlike Delta, creating the table already registers it with the
        # Iceberg catalog (Hive metastore or otherwise), so there's no
        # separate metastore-registration step needed here.
        table_identifier = self._create_iceberg_table_if_not_exists(
            resource_df, resource_type
        )

        with MeasureElapsed(
            table_operations_timer,
            {
                "operation": "merge",
                "resource_type": resource_type,
                "table_format": "iceberg",
            },
        ):
            self._merge_into_iceberg_table(
                resource_df, resource_type, table_identifier, put_count
            )

        if delete_count > 0:
            with MeasureElapsed(
                table_operations_timer,
                {
                    "operation": "delete",
                    "resource_type": resource_type,
                    "table_format": "iceberg",
                },
            ):
                self._delete_from_iceberg_table(
                    delete_df, resource_type, table_identifier, delete_count
                )

        # TODO: should optimize/expire snapshots for all tables, not just the
        # ones in the batch
        if batch_id % self.settings.spark.upkeep_interval == 0:
            self._optimize_and_vacuum_iceberg_table(resource_type)

    def _iceberg_table_ref(self, resource_type: str) -> str:
        """The namespace-qualified table name, without the catalog prefix.

        This is the form expected by the `table` argument of Iceberg's
        system stored procedures (CALL <catalog>.system.<procedure>(...)).
        """
        return f"{self.settings.iceberg.namespace}.{resource_type}"

    def _iceberg_table_identifier(self, resource_type: str) -> str:
        """The fully catalog-qualified table identifier, for SQL DML/DDL."""
        catalog_name = self.settings.iceberg.catalog_name
        return f"{catalog_name}.{self._iceberg_table_ref(resource_type)}"

    @retry(
        wait=wait_exponential(multiplier=1, min=5, max=30),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _create_iceberg_table_if_not_exists(
        self, resource_df: DataFrame, resource_type: str
    ) -> str:
        iceberg = self.settings.iceberg
        namespace = f"{iceberg.catalog_name}.{iceberg.namespace}"
        table_identifier = self._iceberg_table_identifier(resource_type)

        self.pc.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        table_settings = iceberg.tables.get(resource_type)

        partition_parts = []

        bucket_column = table_settings.bucket_column if table_settings else ""
        bucket_count = table_settings.bucket_count if table_settings else 0
        if bucket_column and bucket_count:
            partition_parts.append(f"bucket({bucket_count}, {bucket_column})")
        elif bucket_column or bucket_count:
            logger.warning(
                "Ignoring incomplete bucket partitioning config for "
                "{resource_type}: both bucket_column and bucket_count must "
                "be set",
                resource_type=resource_type,
            )

        partition_columns = table_settings.partition_columns if table_settings else []
        if partition_columns:
            partition_parts.extend(partition_columns)

        partition_clause = (
            f"PARTITIONED BY ({', '.join(partition_parts)})" if partition_parts else ""
        )

        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS {table_identifier} "
            f"({resource_df.schema.toDDL()}) "
            f"USING iceberg {partition_clause} "
            "TBLPROPERTIES ("
            f"'format-version'='{iceberg.format_version}', "
            f"'write.target-file-size-bytes'='{iceberg.target_file_size_bytes}', "
            f"'write.distribution-mode'='{iceberg.write_distribution_mode}', "
            f"'write.merge.mode'='{iceberg.write_merge_mode}', "
            f"'write.update.mode'='{iceberg.write_update_mode}', "
            f"'write.delete.mode'='{iceberg.write_delete_mode}'"
            ")"
        )
        logger.info(create_table_query)
        self.pc.spark.sql(create_table_query)

        sort_columns = table_settings.sort_columns if table_settings else []
        if sort_columns:
            self.pc.spark.sql(
                f"ALTER TABLE {table_identifier} WRITE ORDERED BY "
                f"{', '.join(sort_columns)}"
            )
            # WRITE ORDERED BY switches write.distribution-mode to 'range' as
            # a side effect, which would force a full global shuffle+sort on
            # every micro-batch merge. Reset it back so regular batches stay
            # cheap (locally sorted per task) and only the periodic
            # sort-strategy compaction pays for a full re-sort.
            self.pc.spark.sql(
                f"ALTER TABLE {table_identifier} SET TBLPROPERTIES "
                f"('write.distribution-mode'='{iceberg.write_distribution_mode}')"
            )

        return table_identifier

    @retry(
        wait=wait_exponential(multiplier=1, min=5, max=30),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _merge_into_iceberg_table(
        self,
        resource_df: DataFrame,
        resource_type: str,
        table_identifier: str,
        resources_count: int,
    ):
        logger.info(
            "Merging into table {resource_type} with {resources_count} rows",
            resource_type=resource_type,
            resources_count=resources_count,
        )

        view_name = f"__fhir_to_lakehouse_merge_source_{resource_type}"
        resource_df.createOrReplaceTempView(view_name)
        try:
            self.pc.spark.sql(
                f"MERGE INTO {table_identifier} t "
                f"USING {view_name} s "
                "ON s.id = t.id "
                "WHEN MATCHED THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *"
            )
        finally:
            self.pc.spark.catalog.dropTempView(view_name)

        resources_processed_counter.add(
            resources_count,
            {
                "operation": "written",
                "resource_type": resource_type,
                "table_format": "iceberg",
            },
        )

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _delete_from_iceberg_table(
        self,
        delete_df: DataFrame,
        resource_type: str,
        table_identifier: str,
        deletes_count: int,
    ):
        logger.info(
            "Deleting from table {resource_type} with {delete_df_size} rows",
            resource_type=resource_type,
            delete_df_size=deletes_count,
        )

        view_name = f"__fhir_to_lakehouse_delete_source_{resource_type}"
        delete_df.createOrReplaceTempView(view_name)
        try:
            self.pc.spark.sql(
                f"MERGE INTO {table_identifier} t "
                f"USING {view_name} s "
                "ON s.request_resource_id = t.id "
                "WHEN MATCHED THEN DELETE"
            )
        finally:
            self.pc.spark.catalog.dropTempView(view_name)

        resources_processed_counter.add(
            deletes_count,
            {
                "operation": "delete",
                "resource_type": resource_type,
                "table_format": "iceberg",
            },
        )

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARN),  # type: ignore
    )
    def _optimize_and_vacuum_iceberg_table(self, resource_type: str):
        logger.info("Optimizing and expiring snapshots for table")

        catalog_name = self.settings.iceberg.catalog_name
        table_ref = self._iceberg_table_ref(resource_type)

        # use the table's default sort order (set via WRITE ORDERED BY in
        # _create_iceberg_table_if_not_exists) to actually re-cluster files
        # during this periodic maintenance pass; regular streaming writes
        # only sort locally per task and don't fix up the whole table's
        # file layout on their own.
        table_settings = self.settings.iceberg.tables.get(resource_type)
        sort_columns = table_settings.sort_columns if table_settings else []
        strategy_clause = ", strategy => 'sort'" if sort_columns else ""

        with MeasureElapsed(
            table_operations_timer,
            {
                "operation": "optimize",
                "resource_type": resource_type,
                "table_format": "iceberg",
            },
        ):
            rewrite_df = self.pc.spark.sql(
                f"CALL {catalog_name}.system.rewrite_data_files("
                f"table => '{table_ref}'{strategy_clause})"
            )

        logger.info(
            "Finished optimizing table. Statistics: {stats}",
            stats=rewrite_df.toJSON().collect(),
        )

        older_than = datetime.now(UTC) - timedelta(
            hours=self.settings.vacuum_retention_hours
        )
        older_than_literal = older_than.strftime("%Y-%m-%d %H:%M:%S.%f")

        with MeasureElapsed(
            table_operations_timer,
            {
                "operation": "vacuum",
                "resource_type": resource_type,
                "table_format": "iceberg",
            },
        ):
            expire_df = self.pc.spark.sql(
                f"CALL {catalog_name}.system.expire_snapshots("
                f"table => '{table_ref}', "
                f"older_than => TIMESTAMP '{older_than_literal}', "
                "retain_last => 1)"
            )

        logger.info(
            "Finished vacuuming table. Statistics: {stats}",
            stats=expire_df.toJSON().collect(),
        )
