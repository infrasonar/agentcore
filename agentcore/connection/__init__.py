import asyncio
import logging
import os
from .probeserverprotocol import ProbeServerProtocol


PROBE_SERVER_PORT = int(os.getenv('PROBE_SERVER_PORT', 8750))


def init_probe_server(loop: asyncio.AbstractEventLoop) -> \
        asyncio.AbstractServer:
    host, port = None, PROBE_SERVER_PORT

    coro = loop.create_server(ProbeServerProtocol, host=host, port=port)
    binding = '*' if host is None else host
    logging.info(f'listening for probes on {binding}:{port}')
    return loop.run_until_complete(coro)
