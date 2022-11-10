import asyncio
import json
import logging
import os
import ssl
from .net.package import Package
from .protocol import HubProtocol

HUB_QUEUE_SIZE = 100000
HUB_QUEUE_SLEEP = .001

HUB_HOST = os.getenv('HUB_HOST', 'hub.infrasonar.com')
HUB_PORT = int(os.getenv('HUB_PORT', 8730))

AGENTCORE_ID_FN = os.getenv('AGENTCORE_JSON', '/data/.agentcore.json')
if not os.path.exists(AGENTCORE_ID_FN):
    logging.info('agentcore id file not found. creating a new one')
    try:
        if not os.path.exists(os.path.dirname(AGENTCORE_ID_FN)):
            os.makedirs(os.path.dirname(AGENTCORE_ID_FN))
        with open(AGENTCORE_ID_FN, 'w') as fp:
            json.dump(None, fp)
    except Exception:
        logging.exception('failed to create agentcore_id file\n')
        exit(1)
AGENTCORE_HUB_CRT = os.getenv('AGENTCORE_HUB_CRT', 'certificates/hub.crt')
if not os.path.exists(AGENTCORE_HUB_CRT):
    logging.error(f'file does not exist: {AGENTCORE_HUB_CRT}')
    exit(1)


class HubClient:

    def __init__(
        self,
        container_id: int,
        agentcore_id: int,
        zone: int,
        token: str,
    ):
        self.container_id = container_id
        self.agentcore_id = agentcore_id or self._read_id()
        self.zone = zone
        self.token = token
        self.queue = asyncio.Queue(maxsize=HUB_QUEUE_SIZE)
        self._connecting = False
        self._protocol = None
        self._queue_fut = None

    def is_connected(self) -> bool:
        return self._protocol is not None and self._protocol.is_connected()

    def is_connecting(self) -> bool:
        return self._connecting

    async def start(self):
        initial_step = 2
        step = 2
        max_step = 2 ** 7

        while 1:
            if not self.is_connected() and not self.is_connecting():
                asyncio.ensure_future(self._connect())
                step = min(step * 2, max_step)
            else:
                step = initial_step
            await asyncio.sleep(step)

    async def _connect(self):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.load_verify_locations(AGENTCORE_HUB_CRT)

        conn = asyncio.get_event_loop().create_connection(
            HubProtocol,
            host=HUB_HOST,
            port=HUB_PORT,
            ssl=ctx
        )
        self._connecting = True

        try:
            _, self._protocol = await asyncio.wait_for(conn, timeout=10)
        except Exception as e:
            logging.error(f'connecting to hub failed: {e}')
        else:
            pkg = Package.make(
                HubProtocol.PROTO_REQ_ANNOUNCE,
                data=[
                    self.container_id,
                    self.agentcore_id,
                    self.zone,
                    self.token
                ]
            )
            if self._protocol and self._protocol.transport:
                try:
                    id = await self._protocol.request(pkg, timeout=10)
                except Exception as e:
                    logging.error(e)
                else:
                    if self.agentcore_id is None:
                        self.agentcore_id = id
                        self._on_announced_new_id(id)

                    if self._queue_fut is None or self._queue_fut.done():
                        self._queue_fut = \
                            asyncio.ensure_future(self._empty_queue_loop())
        finally:
            self._connecting = False

    async def _empty_queue_loop(self):
        while self.is_connected():
            pkg = await self.queue.get()

            pkg = Package.make(
                HubProtocol.PROTO_REQ_DATA,
                data=pkg.body,
                partid=pkg.partid,
                is_binary=True
            )
            try:
                await self._protocol.request(pkg, timeout=10)
            except Exception as e:
                logging.error(e)

            await asyncio.sleep(HUB_QUEUE_SLEEP)

    def _read_id(self):
        with open(AGENTCORE_ID_FN) as fp:
            json.load(fp)

    def _on_announced_new_id(self, id):
        with open(AGENTCORE_ID_FN, 'w') as fp:
            json.dump(id, fp)

    def close(self):
        if self._queue_fut is not None:
            self._queue_fut.cancel()
        if self._protocol and self._protocol.transport:
            self._protocol.transport.close()
        self._protocol = None
