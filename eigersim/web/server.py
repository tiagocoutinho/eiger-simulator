import asyncio

import click
import uvicorn

from .api import app, Detector


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
    default="tcp://0:9999",
    help="Bind ZMQ socket",
    show_default=True,
)
def main(host: str, port: int, zmq: str):
    detector = Detector(zmq_bind=zmq)
    app.detector = detector
    uvicorn.run(app, host=host, port=port)
