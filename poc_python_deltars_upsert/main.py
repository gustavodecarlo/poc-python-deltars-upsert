import logging

import typer

from poc_python_deltars_upsert.dataflow import (
    dummy_dataflow,
    ingest_json_kafka_dataflow,
    read_kafka_sink_delta,
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s',
    level='DEBUG'
)
logger = logging.getLogger(__name__)

app = typer.Typer()

@app.command()
def version():
    logger.info("version")

@app.command()
def dummy_deltalake_upsert():
   dummy_dataflow()

@app.command()
def ingest_kafka_dataflow(
   kafka_server: str = typer.Option(
        ...,
        help="Kafka server URI",
    ),
    topic: str = typer.Option(
        ...,
        help="Kafka Topic",
    ),
):
   kwargs = locals()
   ingest_json_kafka_dataflow(**kwargs)

@app.command()
def read_kafka_dataflow(
   kafka_server: str = typer.Option(
        ...,
        help="Kafka server URI",
    ),
    topic: str = typer.Option(
        ...,
        help="Kafka Topic",
    ),
    consumer_group: str = typer.Option(
        ...,
        help="Kafka Topic Consumer Group",
    ),
):
   kwargs = locals()
   read_kafka_sink_delta(**kwargs)

if __name__ == "__main__":
    app()
