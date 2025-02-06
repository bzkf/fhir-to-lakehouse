import os

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
    min_offsets_per_trigger: int = 10000
    max_trigger_delay: str = "15m"
    security_protocol: str = "PLAINTEXT"


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


@ts.settings
class DeltaSettings:
    auto_optimize_auto_compact: str = "true"
    auto_optimize_optimize_write: str = "true"
    enable_deletion_vectors: str = "true"


@ts.settings
class Settings:
    kafka: KafkaSettings
    spark: SparkSettings
    delta: DeltaSettings
    aws_access_key_id: str = "admin"
    aws_secret_access_key: str = ts.secret(default="miniopass")
    delta_database_dir: str = "s3a://fhir/warehouse"
    vacuum_retention_hours: int = 24
    metrics_port: int = 8000
    metrics_addr: str = "127.0.0.1"
    metastore_url: str = ""


settings = ts.load(Settings, appname="fhir_to_lakehouse", env_prefix="")
