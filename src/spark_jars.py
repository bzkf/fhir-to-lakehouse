import argparse
import glob
import os

from pyspark.sql import SparkSession

PACKAGES = [
    "au.csiro.pathling:library-runtime:9.8.0",
    "io.delta:delta-spark_2.13:4.0.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2",
    "org.apache.hadoop:hadoop-aws:3.4.1",
]

# already shipped as part of pyspark's own jars
EXCLUDES = [
    "org.apache.hadoop:hadoop-client-api",
    "org.apache.hadoop:hadoop-client-runtime",
]

# Spark's built-in Hive 2.3 metastore client calls the `get_table` Thrift method,
# which was removed in Hive Metastore 4.1. 4.0.1 is the most recent client version
# supported by Spark 4.0 and uses `get_table_req` instead.
HIVE_METASTORE_VERSION = "4.0.1"

# the Hive client artifacts Spark resolves for `spark.sql.hive.metastore.jars=maven`,
# see <https://github.com/apache/spark/blob/v4.0.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/client/IsolatedClientLoader.scala>
HIVE_METASTORE_PACKAGES = [
    f"org.apache.hive:{artifact}:{HIVE_METASTORE_VERSION}"
    for artifact in ["hive-metastore", "hive-exec", "hive-common", "hive-serde"]
]

# not available on Maven Central, see `hive.v4_0` in
# <https://github.com/apache/spark/blob/v4.0.2/sql/hive/src/main/scala/org/apache/spark/sql/hive/client/package.scala>
HIVE_METASTORE_EXCLUDES = [
    "org.apache.calcite:calcite-druid",
    "org.apache.curator:*",
    "org.pentaho:pentaho-aggdesigner-algorithm",
    "org.apache.hive:hive-vector-code-gen",
    # the isolated Hive client classloader always loads Hadoop classes from
    # Spark's own classpath. Also avoids resolving the non-existent
    # hadoop-client-api:2.7.3
    "org.apache.hadoop:*",
]


def with_jars(builder: SparkSession.Builder) -> SparkSession.Builder:
    builder = builder.config("spark.sql.hive.metastore.version", HIVE_METASTORE_VERSION)

    # inside the container image, the jars are downloaded at build time
    # and only need to be put on the classpath
    jars_dir = os.getenv("SPARK_JARS_DIR")
    if jars_dir:
        jars = sorted(glob.glob(os.path.join(jars_dir, "*.jar")))
        builder = builder.config("spark.jars", ",".join(jars))
    else:
        builder = builder.config("spark.jars.packages", ",".join(PACKAGES)).config(
            "spark.jars.excludes", ",".join(EXCLUDES)
        )

    hive_metastore_jars_dir = os.getenv("SPARK_HIVE_METASTORE_JARS_DIR")
    if hive_metastore_jars_dir:
        return builder.config("spark.sql.hive.metastore.jars", "path").config(
            "spark.sql.hive.metastore.jars.path",
            f"file://{os.path.join(hive_metastore_jars_dir, '*.jar')}",
        )

    # only downloaded once the Hive catalog is actually used
    return builder.config("spark.sql.hive.metastore.jars", "maven")


if __name__ == "__main__":
    # downloads the packages and their dependencies to `<ivy_dir>/jars`.
    # Used when building the container image.
    parser = argparse.ArgumentParser()
    parser.add_argument("ivy_dir")
    parser.add_argument(
        "--hive-metastore",
        action="store_true",
        help="download the Hive metastore client jars instead of the Spark packages",
    )
    args = parser.parse_args()

    if args.hive_metastore:
        packages, excludes = HIVE_METASTORE_PACKAGES, HIVE_METASTORE_EXCLUDES
    else:
        packages, excludes = PACKAGES, EXCLUDES

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.jars.excludes", ",".join(excludes))
        .config("spark.jars.ivy", args.ivy_dir)
        .getOrCreate()
    )
    spark.stop()
