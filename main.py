import asyncio
import logging
import os
import socket
import signal
from setproctitle import setproctitle
from agentcore.client import Agentcore
from agentcore.connection import init_probe_server
from agentcore.logger import setup_logger
from agentcore.state import State

FQDN = socket.getaddrinfo(
    socket.gethostname(),
    0,
    flags=socket.AI_CANONNAME)[0][3]
TOKEN = os.getenv('TOKEN')
AGENTCORE_ZONE = int(os.getenv('AGENTCORE_ZONE', 0))
AGENTCORE_NAME = os.getenv('AGENTCORE_NAME', FQDN)


def stop(signame, *args):
    logging.warning(f'Signal \'{signame}\' received, stop agentcore')
    for task in asyncio.all_tasks():
        task.cancel()
    asyncio.get_event_loop().stop()


if __name__ == '__main__':
    assert TOKEN

    setproctitle('agentcore')
    setup_logger()

    State.name = AGENTCORE_NAME
    State.token = TOKEN
    State.zone = AGENTCORE_ZONE
    State.agentcore = Agentcore()

    loop = asyncio.get_event_loop()
    init_probe_server(loop)

    loop.run_until_complete(State.agentcore.start())

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    loop.run_forever()

    State.stop()

    loop.close()
    logging.info('Bye!')
