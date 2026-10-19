import glob
import os
import sys

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


def with_jars(builder: SparkSession.Builder) -> SparkSession.Builder:
    # inside the container image, the jars are downloaded at build time
    # and only need to be put on the classpath
    jars_dir = os.getenv("SPARK_JARS_DIR")
    if jars_dir:
        jars = sorted(glob.glob(os.path.join(jars_dir, "*.jar")))
        return builder.config("spark.jars", ",".join(jars))

    return builder.config("spark.jars.packages", ",".join(PACKAGES)).config(
        "spark.jars.excludes", ",".join(EXCLUDES)
    )


if __name__ == "__main__":
    # downloads the packages and their dependencies to `<ivy_dir>/jars`.
    # Used when building the container image.
    ivy_dir = sys.argv[1]
    spark = (
        with_jars(SparkSession.builder.master("local[1]"))
        .config("spark.jars.ivy", ivy_dir)
        .getOrCreate()
    )
    spark.stop()
