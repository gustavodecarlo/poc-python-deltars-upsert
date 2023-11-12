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

## Reference

- [deltars python](https://delta.io/blog/2023-10-22-delta-rs-python-v0.12.0/)