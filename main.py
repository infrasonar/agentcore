import asyncio
import logging
import os
import signal
from setproctitle import setproctitle
from agentcore.client import HubClient
from agentcore.connection import init_probe_server
from agentcore.logger import setup_logger
from agentcore.state import State

CONTAINER_ID = int(os.getenv('CONTAINER_ID', 0))
AGENTCORE_ID = int(os.getenv('AGENTCORE_ID', 0))
ZONE_ID = int(os.getenv('ZONE_ID', 0))
TOKEN = os.getenv('TOKEN')


def stop(signame, *args):
    logging.warning(f'Signal \'{signame}\' received, stop agentcore')
    for task in asyncio.all_tasks():
        task.cancel()
    asyncio.get_event_loop().stop()


if __name__ == '__main__':
    assert CONTAINER_ID
    assert TOKEN

    setproctitle('agentcore')
    setup_logger()

    State.hubclient = HubClient(
        CONTAINER_ID,
        AGENTCORE_ID,
        ZONE_ID,
        TOKEN,
    )

    loop = asyncio.get_event_loop()
    init_probe_server(loop)

    loop.run_until_complete(State.hubclient.start())

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    loop.run_forever()

    State.stop()

    loop.close()
    logging.info('Bye!')
