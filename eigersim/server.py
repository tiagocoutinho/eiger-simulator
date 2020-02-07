import asyncio
import logging

import click
import uvicorn

from .web import app, Detector


@click.command()
@click.option(
    "--host",
    type=str,
    default="0",
    help="Bind web socket to this host.",
    show_default=True,
)
@click.option(
    "--port",
    type=int,
    default=8000,
    help="Bind web socket to this port.",
    show_default=True,
)
@click.option(
    "--zmq",
    type=str,
    default="tcp://*:9999",
    help="Bind ZMQ socket",
    show_default=True,
)
@click.option(
    "--dataset",
    type=click.Path(),
    default=None,
    help="dataset path or file",
)
@click.option(
    "--max-memory",
    type=int,
    default=1_000_000_000,
    help="max memory (bytes)",
    show_default=True
)
@click.option(
    "--log-level",
    type=click.Choice(['critical', 'error', 'warning', 'info',
                       'debug', 'trace'], case_sensitive=False),
    default='info',
    help="Show only logs with priority LEVEL or above",
    show_default=True
)
def main(host: str, port: int, zmq: str, dataset: click.Path, max_memory: int, log_level: str):
    fmt = '%(threadName)-10s %(asctime)-15s %(levelname)-5s %(name)s: %(message)s'
    logging.basicConfig(level=log_level.upper(), format=fmt)
    detector = Detector(zmq_bind=zmq, dataset=dataset, max_memory=max_memory)
    app.detector = detector
    uvicorn.run(app, host=host, port=port)


if __name__ == '__main__':
    main()
