import os
from typing import Literal

import typed_settings as ts

HERE = os.path.abspath(os.path.dirname(__file__))


@ts.settings
class KafkaSslSettings:
    truststore_type: str = "PKCS12"
    trust_store_location: str = "/opt/kafka-certs/ca.p12"
    trust_store_password: str = ts.secret(default="")
    keystore_type: str = "PKCS12"
    key_store_location: str = "/opt/kafka-certs/user.p12"
    key_store_password: str = ts.secret(default="")


@ts.settings
class KafkaSettings:
    ssl: KafkaSslSettings
    bootstrap_servers: str = "localhost:9094"
    topics: str = "fhir.msg"
    max_offsets_per_trigger: int = 10000
    min_offsets_per_trigger: int = 1
    max_trigger_delay: str = "15m"
    security_protocol: str = "PLAINTEXT"
    fail_on_data_loss: bool = True


@ts.settings
class SparkSettings:
    install_packages_and_exit: bool = False
    master: str = "local[*]"
    s3_endpoint: str = "localhost:9000"
    s3_connection_ssl_enabled: str = "false"
    warehouse_dir: str = os.path.join(HERE, "warehouse")
    checkpoint_dir: str = "s3a://fhir/checkpoint"
    driver_memory: str = "4g"
    upkeep_interval: int = 50
    streaming_processing_time: str = "0 seconds"
    output_mode: str = "append"


@ts.settings
class DeltaSettings:
    auto_optimize_auto_compact: str = "false"
    auto_optimize_optimize_write: str = "false"
    checkpoint_interval: str = "100"
    checkpoint_write_stats_as_json: str = "false"
    checkpoint_write_stats_as_struct: str = "true"
    clustering_columns_by_resource_type: dict[str, list[str]] = {}


@ts.settings
class IcebergSettings:
    # name of the Spark catalog used for Iceberg tables. Kept separate from
    # `spark_catalog` so Delta and Iceberg tables can coexist in the same
    # Spark session.
    catalog_name: str = "iceberg"
    # "hadoop" stores catalog metadata directly under `iceberg_database_dir`
    # without requiring a Hive metastore. Use "hive" together with
    # `metastore_url` to register tables in a Hive metastore instead.
    catalog_type: str = "hadoop"
    namespace: str = "default"
    format_version: str = "2"
    target_file_size_bytes: str = str(128 * 1024 * 1024)
    write_distribution_mode: str = "hash"
    partition_columns_by_resource_type: dict[str, list[str]] = {}


@ts.settings
class Settings:
    kafka: KafkaSettings
    spark: SparkSettings
    delta: DeltaSettings
    iceberg: IcebergSettings
    # which table format to upsert FHIR resources into
    table_format: Literal["delta", "iceberg"] = "delta"
    aws_access_key_id: str = "admin"
    aws_secret_access_key: str = ts.secret(default="miniopass")
    delta_database_dir: str = "s3a://fhir/warehouse"
    iceberg_database_dir: str = "s3a://fhir/warehouse-iceberg"
    vacuum_retention_hours: int = 24
    metrics_port: int = 8000
    metrics_addr: str = "127.0.0.1"
    metastore_url: str = ""
    # if enabled, log the number of resources per Kafka
    # topic per batch
    log_resource_count_by_source_topic: bool = False
    resource_types_to_process_in_parallel: list[str] = [
        "Patient",
        "Observation",
        "Encounter",
        "Condition",
        "Procedure",
    ]


settings = ts.load(Settings, appname="fhir-to-lakehouse", env_prefix="")
