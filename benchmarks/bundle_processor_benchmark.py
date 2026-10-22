"""
Compares the throughput and memory usage of two versions of the BundleProcessor.

The micro-batches are read from Kafka with the same per-type filters main.py uses,
and each filtered DataFrame is processed in its own thread to mimic the concurrently
running streaming queries. Every run uses a fresh JVM and empty Delta tables.

Usage (requires Java 21 and `docker compose up -d kafka`):

    uv run python benchmarks/bundle_processor_benchmark.py load
    uv run python benchmarks/bundle_processor_benchmark.py compare --baseline-ref <ref>
"""

import argparse
import concurrent.futures
import contextlib
import json
import math
import os
import random
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BOOTSTRAP_SERVERS = "localhost:9094"
TOPIC = "fhir-to-lakehouse-benchmark"
PARTITIONS = 12
RESOURCE_TYPES_TO_PROCESS_IN_PARALLEL = [
    "Patient",
    "Observation",
    "Encounter",
    "Condition",
    "Procedure",
]


def load(args):
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.delete_topics([TOPIC])
        time.sleep(2)
    except UnknownTopicOrPartitionError:
        pass
    with contextlib.suppress(TopicAlreadyExistsError):
        admin.create_topics(
            [
                NewTopic(
                    TOPIC,
                    num_partitions=PARTITIONS,
                    replication_factor=1,
                    topic_configs={"max.message.bytes": str(100 * 1024 * 1024)},
                )
            ]
        )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        max_request_size=100 * 1024 * 1024,
        compression_type="gzip",
    )

    bundles = [
        json.loads(line)
        for line in (REPO_ROOT / "data/bundles.ndjson").read_text().splitlines()
        if line.strip()
    ]

    rng = random.Random(42)
    sent = {"insert": 0, "update": 0, "delete": 0}

    # round 1: inserts into empty tables
    for bundle in bundles:
        producer.send(TOPIC, json.dumps(bundle).encode())
        sent["insert"] += len(bundle["entry"])

    # round 2: the same bundles again (updates), with a share of the
    # non-Patient entries turned into DELETEs
    for bundle in bundles:
        for entry in bundle["entry"]:
            is_patient = entry["request"]["url"].startswith("Patient/")
            if not is_patient and rng.random() < args.delete_ratio:
                entry.pop("resource", None)
                entry["request"]["method"] = "DELETE"
                sent["delete"] += 1
            else:
                sent["update"] += 1
        producer.send(TOPIC, json.dumps(bundle).encode())

    producer.flush()
    print(f"Loaded {2 * len(bundles)} bundles into '{TOPIC}': {sent}")


def offset_ranges(batch_size: int) -> list[tuple[dict, dict]]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
    tps = [TopicPartition(TOPIC, p) for p in consumer.partitions_for_topic(TOPIC)]
    starts = consumer.beginning_offsets(tps)
    ends = consumer.end_offsets(tps)
    consumer.close()

    per_partition = math.ceil(batch_size / len(tps))
    num_batches = max(math.ceil((ends[tp] - starts[tp]) / per_partition) for tp in tps)

    ranges = []
    for i in range(num_batches):
        start, end = {}, {}
        for tp in tps:
            start[str(tp.partition)] = min(starts[tp] + i * per_partition, ends[tp])
            end[str(tp.partition)] = min(starts[tp] + (i + 1) * per_partition, ends[tp])
        ranges.append(({TOPIC: start}, {TOPIC: end}))
    return ranges


class JvmMemorySampler(threading.Thread):
    def __init__(self, jvm, interval: float = 0.2):
        super().__init__(daemon=True)
        management_factory = jvm.java.lang.management.ManagementFactory
        self.memory_bean = management_factory.getMemoryMXBean()
        self.heap_pools = [
            pool
            for pool in management_factory.getMemoryPoolMXBeans()
            if pool.getType().name() == "HEAP"
        ]
        self.interval = interval
        # includes garbage, so it mostly grows towards the max heap size
        self.peak_heap_used = 0
        # heap usage right after the latest GC, an approximation of the live data
        self.peak_heap_used_after_gc = 0
        self.stopped = threading.Event()

    def run(self):
        while not self.stopped.is_set():
            used = self.memory_bean.getHeapMemoryUsage().getUsed()
            self.peak_heap_used = max(self.peak_heap_used, used)
            used_after_gc = sum(
                pool.getCollectionUsage().getUsed()
                for pool in self.heap_pools
                if pool.getCollectionUsage() is not None
            )
            self.peak_heap_used_after_gc = max(
                self.peak_heap_used_after_gc, used_after_gc
            )
            time.sleep(self.interval)


def run(args):
    sys.path.insert(0, str(Path(args.src).resolve()))

    from delta import DeltaTable
    from loguru import logger
    from pathling import PathlingContext
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from bundle_processor import BundleProcessor
    from settings import (
        DeltaSettings,
        KafkaSettings,
        KafkaSslSettings,
        Settings,
        SparkSettings,
    )
    from spark_jars import with_jars

    logger.remove()
    logger.add(sys.stderr, level="WARNING")

    work_dir = Path(tempfile.mkdtemp(prefix="fhir-to-lakehouse-benchmark-"))

    builder = (
        with_jars(SparkSession.builder.master(args.master))
        .appName("fhir_to_lakehouse_benchmark")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.ui.showConsoleProgress", "false")
        # the job and stage counts are read from the UI's REST API
        .config("spark.ui.retainedJobs", "1000000")
        .config("spark.ui.retainedStages", "1000000")
        # otherwise the peak memory metrics are only sampled on each heartbeat
        .config("spark.executor.metrics.pollingInterval", "100ms")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(work_dir / "spark-warehouse"))
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )
    for conf in args.conf:
        key, value = conf.split("=", 1)
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    pc = PathlingContext.create(
        spark,
        enable_extensions=True,
        enable_delta=True,
        enable_terminology=False,
        terminology_server_url="http://localhost/not-a-real-server",
    )

    settings = Settings(
        delta_database_dir=str(work_dir / "warehouse"),
        spark=SparkSettings(upkeep_interval=sys.maxsize),
        delta=DeltaSettings(),
        kafka=KafkaSettings(ssl=KafkaSslSettings()),
        resource_types_to_process_in_parallel=RESOURCE_TYPES_TO_PROCESS_IN_PARALLEL,
    )
    processor = BundleProcessor(pc, settings)

    queries = [
        (f"process_{rt}", F.col("resource_type") == rt)
        for rt in RESOURCE_TYPES_TO_PROCESS_IN_PARALLEL
    ] + [
        (
            "process_default",
            ~F.col("resource_type").isin(RESOURCE_TYPES_TO_PROCESS_IN_PARALLEL),
        )
    ]

    ranges = offset_ranges(args.batch_size)

    jvm = spark.sparkContext._jvm
    assert jvm is not None
    sampler = JvmMemorySampler(jvm)
    sampler.start()

    gc_beans = list(
        jvm.java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()
    )
    gc_time_before = sum(bean.getCollectionTime() for bean in gc_beans)

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(queries)) as pool:
        for batch_id, (starting, ending) in enumerate(ranges, start=1):
            df = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", TOPIC)
                .option("startingOffsets", json.dumps(starting))
                .option("endingOffsets", json.dumps(ending))
                .option("includeHeaders", "true")
                .load()
            )
            df_result = processor.prepare_stream(df)
            futures = [
                pool.submit(
                    processor.process_batch,
                    df_result.filter(condition),
                    batch_id,
                    query_name,
                )
                for query_name, condition in queries
            ]
            for future in futures:
                future.result()
    elapsed = time.perf_counter() - started

    gc_time = sum(bean.getCollectionTime() for bean in gc_beans) - gc_time_before
    sampler.stopped.set()
    sampler.join()

    pid = jvm.java.lang.ProcessHandle.current().pid()
    peak_rss_kib = next(
        int(line.split()[1])
        for line in Path(f"/proc/{pid}/status").read_text().splitlines()
        if line.startswith("VmHWM:")
    )

    sc = spark.sparkContext
    api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    with urllib.request.urlopen(f"{api}/jobs") as response:
        jobs = json.load(response)
    with urllib.request.urlopen(f"{api}/stages") as response:
        stages = json.load(response)
    with urllib.request.urlopen(f"{api}/executors") as response:
        # in local mode, the driver is the only executor
        peak_memory_metrics = json.load(response)[0]["peakMemoryMetrics"]

    table_counts = {}
    for table_dir in sorted((work_dir / "warehouse").glob("*.parquet")):
        name = table_dir.name.removesuffix(".parquet")
        table_counts[name] = DeltaTable.forPath(spark, str(table_dir)).toDF().count()

    print(
        json.dumps(
            {
                "label": args.label,
                "batches": len(ranges),
                "elapsed_seconds": elapsed,
                "kafka_messages": sum(
                    sum(end[TOPIC].values()) - sum(start[TOPIC].values())
                    for start, end in ranges
                ),
                "peak_heap_used_mib": sampler.peak_heap_used / 1024**2,
                "peak_heap_used_after_gc_mib": sampler.peak_heap_used_after_gc
                / 1024**2,
                "peak_spark_execution_memory_mib": peak_memory_metrics[
                    "OnHeapExecutionMemory"
                ]
                / 1024**2,
                "peak_spark_storage_memory_mib": peak_memory_metrics[
                    "OnHeapStorageMemory"
                ]
                / 1024**2,
                "peak_rss_mib": peak_rss_kib / 1024,
                "gc_seconds": gc_time / 1000,
                "spark_jobs": len(jobs),
                "spark_tasks": sum(stage["numCompleteTasks"] for stage in stages),
                "table_counts": table_counts,
            }
        )
    )

    spark.stop()


def compare(args):
    with tempfile.TemporaryDirectory(prefix="fhir-to-lakehouse-baseline-") as tmp:
        baseline_dir = Path(tmp) / "worktree"
        worktree_add = ["git", "worktree", "add", "--detach"]
        subprocess.run(
            [*worktree_add, str(baseline_dir), args.baseline_ref],
            cwd=REPO_ROOT,
            check=True,
        )
        try:
            versions = {
                f"baseline ({args.baseline_ref})": baseline_dir / "src",
                "current (working tree)": REPO_ROOT / "src",
            }
            results: dict[str, list[dict]] = {label: [] for label in versions}

            # alternate the order of the versions to spread out warm-up effects,
            # e.g. the OS page cache or Kafka's log segments
            for i in range(args.runs):
                order = list(versions.items())
                if i % 2 == 1:
                    order.reverse()
                for label, src in order:
                    print(f"run {i + 1}/{args.runs}: {label}", file=sys.stderr)
                    output = subprocess.run(
                        [
                            sys.executable,
                            __file__,
                            "run",
                            "--src",
                            str(src),
                            "--label",
                            label,
                            "--batch-size",
                            str(args.batch_size),
                            "--driver-memory",
                            args.driver_memory,
                            "--master",
                            args.master,
                            *[arg for conf in args.conf for arg in ("--conf", conf)],
                        ],
                        capture_output=True,
                        text=True,
                    )
                    if output.returncode != 0:
                        stderr_tail = output.stderr.splitlines()[-40:]
                        print("\n".join(stderr_tail), file=sys.stderr)
                        output.check_returncode()
                    result = json.loads(output.stdout.strip().splitlines()[-1])
                    print(json.dumps(result), file=sys.stderr)
                    results[label].append(result)
        finally:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(baseline_dir)],
                cwd=REPO_ROOT,
                check=True,
            )

    print_report(results)


def print_report(results: dict[str, list[dict]]):
    baseline_label, current_label = results
    metrics = [
        ("elapsed_seconds", "wall time (s)", "{:.1f}"),
        ("messages_per_second", "throughput (bundles/s)", "{:.1f}"),
        ("peak_heap_used_after_gc_mib", "peak JVM heap after GC (MiB)", "{:.0f}"),
        ("peak_heap_used_mib", "peak JVM heap used (MiB)", "{:.0f}"),
        (
            "peak_spark_execution_memory_mib",
            "peak Spark execution memory (MiB)",
            "{:.0f}",
        ),
        ("peak_spark_storage_memory_mib", "peak Spark storage memory (MiB)", "{:.0f}"),
        ("peak_rss_mib", "peak JVM RSS (MiB)", "{:.0f}"),
        ("gc_seconds", "GC time (s)", "{:.1f}"),
        ("spark_jobs", "Spark jobs", "{:.0f}"),
        ("spark_tasks", "Spark tasks", "{:.0f}"),
    ]
    for runs in results.values():
        for r in runs:
            r["messages_per_second"] = r["kafka_messages"] / r["elapsed_seconds"]

    def median(label, key):
        return statistics.median(r[key] for r in results[label])

    first = results[baseline_label][0]
    print(
        f"\n{len(results[baseline_label])} run(s) per version, "
        f"{first['kafka_messages']} Kafka messages in {first['batches']} batches, "
        "medians:\n"
    )
    print(f"| metric | {baseline_label} | {current_label} | change |")
    print("|---|---:|---:|---:|")
    for key, name, fmt in metrics:
        b, c = median(baseline_label, key), median(current_label, key)
        change = f"{(c - b) / b:+.0%}" if b else "n/a"
        print(f"| {name} | {fmt.format(b)} | {fmt.format(c)} | {change} |")

    counts = {label: runs[0]["table_counts"] for label, runs in results.items()}
    print(f"\ntable row counts: {counts}")
    if counts[baseline_label] != counts[current_label]:
        print("WARNING: the versions produced different table contents")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    load_parser = subparsers.add_parser("load", help="(re)create and fill the topic")
    load_parser.add_argument("--delete-ratio", type=float, default=0.1)
    load_parser.set_defaults(func=load)

    for name, func in (("run", run), ("compare", compare)):
        sub = subparsers.add_parser(name)
        sub.add_argument(
            "--batch-size",
            type=int,
            default=500,
            help="Kafka messages per micro-batch, like maxOffsetsPerTrigger",
        )
        sub.add_argument("--driver-memory", default="4g")
        sub.add_argument("--master", default="local[*]")
        sub.add_argument(
            "--conf", action="append", default=[], help="extra Spark config k=v"
        )
        sub.set_defaults(func=func)

    subparsers.choices["run"].add_argument("--src", required=True)
    subparsers.choices["run"].add_argument("--label", required=True)
    subparsers.choices["compare"].add_argument("--baseline-ref", required=True)
    subparsers.choices["compare"].add_argument("--runs", type=int, default=2)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    os.environ.setdefault("PYSPARK_PIN_THREAD", "true")
    main()
