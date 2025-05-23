import logging
import os
import sys
import socket
import signal
from setproctitle import setproctitle
from agentcore.client import Agentcore
from agentcore.connection import init_probe_server, init_rapp
from agentcore.logger import setup_logger
from agentcore.loop import loop
from agentcore.state import State
from agentcore.version import __version__ as version

try:
    FQDN = socket.getaddrinfo(
        socket.gethostname(),
        0,
        flags=socket.AI_CANONNAME)[0][3]
except Exception:
    FQDN = None

TOKEN = os.getenv('TOKEN')
AGENTCORE_ZONE = int(os.getenv('AGENTCORE_ZONE', 0))
AGENTCORE_NAME = os.getenv('AGENTCORE_NAME', FQDN)

if AGENTCORE_NAME is None:
    sys.exit(
        'Unable to read a name for the agentcore.\n'
        'Please use the `AGENTCORE_NAME` to provide a name.')


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
    init_rapp(loop)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    State.agentcore.start(loop)

    try:
        loop.run_forever()
    except Exception:
        loop.run_until_complete(loop.shutdown_asyncgens())

    logging.info('Bye!')
