import asyncio
import json
import logging
import os
import ssl
import msgpack
from typing import Optional
from .loop import loop
from .net.package import Package
from .protocol import HubProtocol, RespException
from .state import State

HUB_QUEUE_SIZE = 100_000
HUB_QUEUE_SLEEP = .001
HUB_MAX_ERR = 5

HUB_HOST = os.getenv('HUB_HOST', 'hub.infrasonar.com')
HUB_PORT = int(os.getenv('HUB_PORT', 8730))

AGENTCORE_DATA = os.getenv('AGENTCORE_DATA', '/data')
AGENTCORE_JSON_FN = os.path.join(AGENTCORE_DATA, '.agentcore.json')
AGENTCORE_QUEUE_FN = os.path.join(AGENTCORE_DATA, 'queue.mp')
AGENTCORE_ASSETS_FN = os.path.join(AGENTCORE_DATA, 'assets.mp')

if not os.path.exists(AGENTCORE_JSON_FN):
    logging.info('agentcore JSON file not found. creating a new one')
    try:
        if not os.path.exists(os.path.dirname(AGENTCORE_JSON_FN)):
            os.makedirs(os.path.dirname(AGENTCORE_JSON_FN))
        with open(AGENTCORE_JSON_FN, 'w') as fp:
            json.dump(None, fp)
    except Exception:
        logging.exception('failed to create agentcore JSON file\n')
        exit(1)
AGENTCORE_HUB_CRT = os.getenv('AGENTCORE_HUB_CRT', 'certificates/hub.crt')
if not os.path.exists(AGENTCORE_HUB_CRT):
    logging.error(f'file does not exist: {AGENTCORE_HUB_CRT}')
    exit(1)


class Agentcore:
    queue: asyncio.Queue
    _connecting: bool
    _protocol: Optional[HubProtocol]
    _queue_fut: Optional[asyncio.Future]
    _connect_fut: Optional[asyncio.Future]
    _pkg: Optional[Package]

    def __init__(self):
        self.queue = asyncio.Queue(maxsize=HUB_QUEUE_SIZE)
        self._connecting = False
        self._protocol = None
        self._queue_fut = None
        self._connect_fut = None
        self._pkg = None
        self._read_json()

    def is_connected(self) -> bool:
        return self._protocol is not None and self._protocol.is_connected()

    def is_connecting(self) -> bool:
        return self._connecting

    def start(self):
        self.load_queue()
        self._queue_fut = asyncio.ensure_future(self._empty_queue_loop())
        self._connect_fut = asyncio.ensure_future(self._reconnect_loop())

    async def _reconnect_loop(self):
        initial_step = 2
        step = 2
        max_step = 2 ** 7
        while True:
            if not self.is_connected() and not self._connecting:
                asyncio.ensure_future(self._connect())
                step = min(step * 2, max_step)
            else:
                step = initial_step
            for _ in range(step):
                await asyncio.sleep(1)

    async def _connect(self):
        if self._connecting:
            return
        self._connecting = True

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.load_verify_locations(AGENTCORE_HUB_CRT)

        conn = loop.create_connection(
            HubProtocol,
            host=HUB_HOST,
            port=HUB_PORT,
            ssl=ctx
        )

        try:
            _, self._protocol = await asyncio.wait_for(conn, timeout=10)
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(f'connecting to hub failed: {msg}')
            if State.assets_fn is None:
                State.assets_fn = AGENTCORE_ASSETS_FN
                State.load_probe_assets()
                State.remove_assets_fn()
        else:
            pkg = Package.make(
                HubProtocol.PROTO_REQ_ANNOUNCE,
                data=[
                    State.agentcore_id,
                    State.name,
                    State.zone,
                    State.token
                ]
            )
            if self.is_connected():
                try:
                    await self._protocol.request(pkg, timeout=10)
                except Exception as e:
                    msg = str(e) or type(e).__name__
                    logging.error(f'failed to announce: {msg}')
                    self.close_protocol()
                else:
                    self._dump_json()
                    State.assets_fn = AGENTCORE_ASSETS_FN
                    State.remove_assets_fn()
        finally:
            self._connecting = False

    async def _ensure_write_pkg(self):
        """This will write the "current" packe to the hub.
        It will try as long as is required
        """
        err_count = 0
        while True:
            if self.is_connected():
                try:
                    await self._protocol.request(self._pkg, timeout=10)
                except RespException as e:
                    logging.error(f'error from hub: {str(e)}')
                    break
                except TimeoutError as e:
                    msg = str(e) or type(e).__name__
                    logging.error(msg)
                    err_count += 1
                    if err_count % HUB_MAX_ERR == 0 and \
                            self._protocol and \
                            self._protocol.transport:
                        logging.warning(
                            'too many request timeout errors; '
                            'forcing a re-connect')
                        self.close_protocol()
                except Exception as e:
                    msg = str(e) or type(e).__name__
                    logging.exception(msg)
                    err_count += 1
                    if err_count % HUB_MAX_ERR == 0:
                        logging.error('too many errors; skip this request')
                        break
                else:
                    logging.debug('successfully send data to hub')
                    break
            await asyncio.sleep(1)
        self._pkg = None

    async def _empty_queue_loop(self):
        while True:
            pkg = await self.queue.get()

            self._pkg = Package.make(
                HubProtocol.PROTO_REQ_DATA,
                data=pkg.body,
                partid=pkg.partid,
                is_binary=True
            )

            await self._ensure_write_pkg()
            await asyncio.sleep(HUB_QUEUE_SLEEP)

    def _read_json(self):
        with open(AGENTCORE_JSON_FN) as fp:
            State.agentcore_id = json.load(fp)

    def _dump_json(self):
        with open(AGENTCORE_JSON_FN, 'w') as fp:
            json.dump(State.agentcore_id, fp)

    def _read_queue(self):
        if self._pkg is not None:
            yield self._pkg.to_bytes()
        try:
            while True:
                pkg = self.queue.get_nowait()
                yield pkg.to_bytes()
        except asyncio.QueueEmpty:
            pass

    def dump_queue(self):
        logging.info(f'write queue to: {AGENTCORE_QUEUE_FN}')
        with open(AGENTCORE_QUEUE_FN, 'wb') as fp:
            msgpack.pack([pkg for pkg in self._read_queue()], fp)

    def load_queue(self):
        if not os.path.exists(AGENTCORE_QUEUE_FN):
            logging.info('no queue file')
            return
        try:
            with open(AGENTCORE_QUEUE_FN, 'rb') as fp:
                data = msgpack.unpack(fp, use_list=False, strict_map_key=False)
            for barray in data[:HUB_QUEUE_SIZE]:
                pkg = Package.from_bytes(barray)
                self.queue.put_nowait(pkg)
            logging.info(f'read {len(data)} package(s) for queue at startup')
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(
                f'failed loading queue: {AGENTCORE_QUEUE_FN} ({msg})')
            return
        try:
            os.remove(AGENTCORE_QUEUE_FN)
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(f'failed to remove: {AGENTCORE_QUEUE_FN} ({msg})')
        else:
            logging.info(f'removed queue file: {AGENTCORE_QUEUE_FN}')

    def close_protocol(self):
        if self._protocol and self._protocol.transport:
            self._protocol.transport.close()
        self._protocol = None

    def close(self):
        if self._queue_fut is not None:
            self._queue_fut.cancel()
        if self._connect_fut is not None:
            self._connect_fut.cancel()
        self.close_protocol()
        try:
            self.dump_queue()
        except Exception:
            logging.exception('')
