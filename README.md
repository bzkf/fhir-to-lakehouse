# fhir-to-lakehouse

Reads FHIR bundles from Kafka, encodes them using the Pathling encoders, and stores them as Delta Lake tables.

## Development

The [compose.yaml](compose.yaml) contains the development fixtures required to run the program out-of-the-box:

- Apache Kafka (Exposed on <127.0.0.1:9094>)
- Kafbat - an Apache Kafka UI (Exposed on <127.0.0.1:8084>)
- MinIO (Exposed on <127.0.0.1:9000> and <127.0.0.1:9001> for the UI)
- mock-data-loader: used to pre-load Kafka with sample FHIR bundles

start all services using

```sh
docker compose up
```

and the program itself using

```sh
python src/main.py
```
