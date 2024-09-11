import asyncio
import logging
import os
import socket
import signal
from setproctitle import setproctitle
from agentcore.client import Agentcore
from agentcore.connection import init_probe_server
from agentcore.logger import setup_logger
from agentcore.loop import loop
from agentcore.state import State
from agentcore.version import __version__ as version

FQDN = socket.getaddrinfo(
    socket.gethostname(),
    0,
    flags=socket.AI_CANONNAME)[0][3]
TOKEN = os.getenv('TOKEN')
AGENTCORE_ZONE = int(os.getenv('AGENTCORE_ZONE', 0))
AGENTCORE_NAME = os.getenv('AGENTCORE_NAME', FQDN)


def stop(signame, *args):
    logging.warning(f'signal \'{signame}\' received, stop agentcore')
    State.stop()
    loop.close()


if __name__ == '__main__':
    assert TOKEN

    setproctitle('agentcore')
    setup_logger()

    logging.warning(f'stating agentcore v{version}')

    State.name = AGENTCORE_NAME
    State.token = TOKEN
    State.zone = AGENTCORE_ZONE
    State.agentcore = Agentcore()

    init_probe_server(loop)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    State.agentcore.start()

    try:
        loop.run_forever()
    except Exception:
        loop.run_until_complete(loop.shutdown_asyncgens())

    logging.info('Bye!')
