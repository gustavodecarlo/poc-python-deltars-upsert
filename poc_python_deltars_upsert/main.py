import logging

import typer

from poc_python_deltars_upsert.dataflow import (
    dummy_dataflow
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

if __name__ == "__main__":
    app()