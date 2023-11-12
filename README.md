# poc-python-deltars-upsert

Project to test new feature of deltars python library to upsert data in deltalake table.

## Objectives

- Have a dummy example with a batch example saving in a deltalake table on OS filesystem
- Have a stream example with kafka upserting data in a delta table on OS filesystem

## Results

[RESULTS.md](RESULTS.md)

## Using the project

- Requirements:
    - poetry

Install the project

```bash
poetry install
mkdir data
```

Configure Redpanda Kafka

```bash
docker compose up -d
```

Access in browser localhost:8080 and create the topic and run the command below.

```bash
poetry run python poc_python_deltars_upsert/main.py ingest-kafka-dataflow --kafka-server localhost:19092 --topic <topic>
```

To create the `sales_orders` table run the command:

```bash
poetry run python poc_python_deltars_upsert/main.py dummy-deltalake-upsert
```

To test the stream upsert dataflow:

```bash
poetry run python poc_python_deltars_upsert/main.py read-kafka-dataflow --kafka-server localhost:19092 --topic <topic> --consumer-group <consumer-group>
```

## Reference

- [deltars python](https://delta.io/blog/2023-10-22-delta-rs-python-v0.12.0/)