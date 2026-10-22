import json
import threading
import time
import urllib.request
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from loguru import logger
from opentelemetry.metrics import (
    CallbackOptions,
    Histogram,
    Observation,
    get_meter_provider,
)
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
)

meter = get_meter_provider().get_meter("fhir_to_lakehouse.instrumentation")

# the SDK's default buckets are meant for milliseconds
DURATION_SECONDS_BUCKETS = [
    0.1,
    0.25,
    0.5,
    1,
    2.5,
    5,
    7.5,
    10,
    15,
    20,
    30,
    45,
    60,
    90,
    120,
    180,
    300,
    600,
    1200,
]


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
        logger.info(
            "Elapsed time: {elapsed_seconds}",
            elapsed_seconds=self.time,
            **self.attributes,
        )


# https://docs.databricks.com/aws/en/structured-streaming/stream-monitoring
class KafkaOffsetTrackingStreamingQueryListener(StreamingQueryListener):
    def __init__(self):
        self.query_processed_rows_per_second = meter.create_gauge(
            name="spark-streaming-query-processed-rows-per-second",
            unit="{Count}/s",
            description="Processed rows per second by streaming query",
        )
        self.query_kafka_offsets = meter.create_gauge(
            name="spark-streaming-query-kafka-offset",
            unit="{Num}",
            description="Kafka offset by type, streaming query, topic, and partition",
        )
        self.query_input_rows = meter.create_counter(
            name="spark-streaming-query-input-rows",
            unit="{Count}",
            description="Number of Kafka records read by streaming query",
        )
        self.query_batch_duration = meter.create_histogram(
            name="spark-streaming-query-batch-duration",
            unit="s",
            description="Duration of non-empty micro-batches by streaming query",
            explicit_bucket_boundaries_advisory=DURATION_SECONDS_BUCKETS,
        )
        self.query_trigger_phase_duration = meter.create_gauge(
            name="spark-streaming-query-trigger-phase-duration",
            unit="s",
            description="Duration of each phase of the latest trigger by streaming "
            + "query, e.g. addBatch is the time spent in foreachBatch",
        )

    def onQueryStarted(self, event: QueryStartedEvent):
        """
        Called when a query is started.

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryStartedEvent`
            The properties are available as the same as Scala API.

        Notes
        -----
        This is called synchronously with
        meth:`pyspark.sql.streaming.DataStreamWriter.start`,
        that is, ``onQueryStart`` will be called on all listeners before
        ``DataStreamWriter.start()`` returns the corresponding
        :class:`pyspark.sql.streaming.StreamingQuery`.
        Do not block in this method as it will block your query.
        """
        pass

    def onQueryProgress(self, event: QueryProgressEvent):
        """
        Called when there is some status update (ingestion rate updated, etc.)

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryProgressEvent`
            The properties are available as the same as Scala API.

        Notes
        -----
        This method is asynchronous. The status in
        :class:`pyspark.sql.streaming.StreamingQuery` returns the
        most recent status, regardless of when this method is called. The status
        of :class:`pyspark.sql.streaming.StreamingQuery`.
        may change before or when you process the event.
        For example, you may find :class:`StreamingQuery`
        terminates when processing `QueryProgressEvent`.
        """

        logger.info(
            "Streaming Query Progress for {query_name}: {source}",
            query_name=event.progress.name,
            source=json.loads(event.progress.prettyJson),
        )

        # unnamed streaming queries have no name
        query_name = event.progress.name or ""

        self.query_processed_rows_per_second.set(
            event.progress.processedRowsPerSecond, {"query_name": query_name}
        )

        self.query_input_rows.add(
            event.progress.numInputRows, {"query_name": query_name}
        )

        # progress events are also sent periodically for triggers without new data
        if event.progress.numInputRows > 0:
            self.query_batch_duration.record(
                event.progress.batchDuration / 1000, {"query_name": query_name}
            )

        for phase, duration_ms in event.progress.durationMs.items():
            self.query_trigger_phase_duration.set(
                duration_ms / 1000, {"query_name": query_name, "phase": phase}
            )

        for source in event.progress.sources:
            if source.description.startswith("Kafka"):
                if source.startOffset != "None":
                    self._update_kafka_offsets("start", query_name, source.startOffset)
                if source.endOffset != "None":
                    self._update_kafka_offsets("end", query_name, source.endOffset)
                if source.latestOffset != "None":
                    self._update_kafka_offsets(
                        "latest", query_name, source.latestOffset
                    )

    def _update_kafka_offsets(self, type: str, query_name: str, str_offsets: str):
        start_offsets = json.loads(str_offsets)
        for topic, start_offset in start_offsets.items():
            for partition, offset in start_offset.items():
                self.query_kafka_offsets.set(
                    offset,
                    {
                        "type": type,
                        "query_name": query_name,
                        "topic": topic,
                        "partition": partition,
                    },
                )

    def onQueryIdle(self, event: QueryIdleEvent):
        """
        Called when the query is idle and waiting for new data to process.
        """
        pass

    def onQueryTerminated(self, event: QueryTerminatedEvent):
        """
        Called when a query is stopped, with or without error.

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryTerminatedEvent`
            The properties are available as the same as Scala API.
        """
        pass


class SparkRuntimeMetrics:
    """
    Exposes JVM and Spark executor metrics alongside the application metrics, so
    Spark's own metrics endpoints on the UI port don't need to be scraped as well.

    The JVM metrics are read via JMX through py4j, the executor metrics from Spark's
    monitoring REST API. Both are read at most once per `cache_seconds`, no matter
    how many instruments are collected per scrape.
    """

    def __init__(self, spark: SparkSession, cache_seconds: float = 5):
        sc = spark.sparkContext
        jvm: Any = sc._jvm
        if jvm is None:
            raise RuntimeError("The SparkContext isn't backed by a JVM")

        management_factory = jvm.java.lang.management.ManagementFactory
        self.memory_pools = list(management_factory.getMemoryPoolMXBeans())
        self.gc_beans = list(management_factory.getGarbageCollectorMXBeans())
        self.jvm_status_path = Path(
            f"/proc/{jvm.java.lang.ProcessHandle.current().pid()}/status"
        )
        self.executors_url = (
            f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/executors"
            if sc.uiWebUrl
            else None
        )

        self.cache_seconds = cache_seconds
        self.lock = threading.Lock()
        self.snapshot: dict = {}
        self.snapshot_time = 0.0

        def observe(key: str, unit: str, description: str, counter: bool = False):
            create = (
                meter.create_observable_counter
                if counter
                else meter.create_observable_gauge
            )
            create(
                name=key,
                callbacks=[lambda options: self._observations(key, options)],
                unit=unit,
                description=description,
            )

        observe("jvm-memory-used", "By", "JVM memory used by memory pool")
        observe("jvm-memory-committed", "By", "JVM memory committed by memory pool")
        observe("jvm-memory-limit", "By", "Maximum JVM memory by memory pool")
        observe(
            "jvm-memory-used-after-last-gc",
            "By",
            "JVM memory used right after the latest GC by memory pool. "
            + "Approximates the live data on the heap.",
        )
        observe("jvm-gc-collection-time", "s", "Time spent in GC by collector", True)
        observe("jvm-gc-collections", "{Count}", "Number of GCs by collector", True)
        observe(
            "jvm-process-resident-memory",
            "By",
            "Resident memory of the JVM process, including off-heap memory",
        )
        observe(
            "spark-executor-tasks",
            "{Count}",
            "Number of finished Spark tasks by executor and state",
            True,
        )
        observe(
            "spark-executor-active-tasks", "{Count}", "Running Spark tasks by executor"
        )
        observe(
            "spark-executor-storage-memory-used",
            "By",
            "Memory used for cached data (storage memory) by executor",
        )
        observe(
            "spark-executor-storage-memory-limit",
            "By",
            "Memory available for storage and execution by executor",
        )
        observe(
            "spark-executor-shuffle",
            "By",
            "Shuffle data read and written by executor",
            True,
        )

    def _observations(
        self, key: str, options: CallbackOptions
    ) -> Iterable[Observation]:
        return [Observation(value, attributes) for value, attributes in self._get(key)]

    def _get(self, key: str) -> list[tuple[float, dict]]:
        with self.lock:
            if time.monotonic() - self.snapshot_time > self.cache_seconds:
                try:
                    self.snapshot = self._collect()
                except Exception as e:
                    # e.g. while the JVM is shutting down
                    logger.debug("Failed to collect Spark runtime metrics: {e}", e=e)
                    self.snapshot = {}
                self.snapshot_time = time.monotonic()
            return self.snapshot.get(key, [])

    def _collect(self) -> dict[str, list[tuple[float, dict]]]:
        snapshot: dict[str, list[tuple[float, dict]]] = {}

        def add(key: str, value: float, **attributes):
            snapshot.setdefault(key, []).append((value, attributes))

        for pool in self.memory_pools:
            attributes = {
                "type": "heap" if pool.getType().name() == "HEAP" else "non_heap",
                "pool": pool.getName(),
            }
            usage = pool.getUsage()
            add("jvm-memory-used", usage.getUsed(), **attributes)
            add("jvm-memory-committed", usage.getCommitted(), **attributes)
            # -1 if undefined
            if (limit := usage.getMax()) >= 0:
                add("jvm-memory-limit", limit, **attributes)
            # only supported by some pools
            if (collection_usage := pool.getCollectionUsage()) is not None:
                add(
                    "jvm-memory-used-after-last-gc",
                    collection_usage.getUsed(),
                    **attributes,
                )

        for gc in self.gc_beans:
            add(
                "jvm-gc-collection-time", gc.getCollectionTime() / 1000, gc=gc.getName()
            )
            add("jvm-gc-collections", gc.getCollectionCount(), gc=gc.getName())

        if self.jvm_status_path.exists():
            for line in self.jvm_status_path.read_text().splitlines():
                if line.startswith("VmRSS:"):
                    add("jvm-process-resident-memory", int(line.split()[1]) * 1024)

        if self.executors_url:
            with urllib.request.urlopen(self.executors_url, timeout=5) as response:  # nosec B310
                executors = json.load(response)
            for executor in executors:
                executor_id = executor["id"]
                for state in ("completed", "failed"):
                    add(
                        "spark-executor-tasks",
                        executor[f"{state}Tasks"],
                        executor_id=executor_id,
                        state=state,
                    )
                add(
                    "spark-executor-active-tasks",
                    executor["activeTasks"],
                    executor_id=executor_id,
                )
                add(
                    "spark-executor-storage-memory-used",
                    executor["memoryUsed"],
                    executor_id=executor_id,
                )
                add(
                    "spark-executor-storage-memory-limit",
                    executor["maxMemory"],
                    executor_id=executor_id,
                )
                add(
                    "spark-executor-shuffle",
                    executor["totalShuffleRead"],
                    executor_id=executor_id,
                    direction="read",
                )
                add(
                    "spark-executor-shuffle",
                    executor["totalShuffleWrite"],
                    executor_id=executor_id,
                    direction="write",
                )

        return snapshot
