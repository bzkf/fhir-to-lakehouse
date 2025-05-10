import json
import time

from loguru import logger
from opentelemetry.metrics import Histogram, get_meter_provider
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
)

meter = get_meter_provider().get_meter("fhir_to_lakehouse.instrumentation")


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

        self.query_processed_rows_per_second.set(
            event.progress.processedRowsPerSecond, {"query_name": event.progress.name}
        )

        for source in event.progress.sources:
            if source.description.startswith("Kafka"):
                if source.startOffset != "None":
                    self._update_kafka_offsets(
                        "start", event.progress.name, source.startOffset
                    )
                if source.endOffset != "None":
                    self._update_kafka_offsets(
                        "end", event.progress.name, source.endOffset
                    )
                if source.latestOffset != "None":
                    self._update_kafka_offsets(
                        "latest", event.progress.name, source.latestOffset
                    )

    def _update_kafka_offsets(self, type: str, query_name: str, str_offsets: str):
        start_offset = json.loads(str_offsets)
        for topic, offset in start_offset.items():
            for partition, offset in offset.items():
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
