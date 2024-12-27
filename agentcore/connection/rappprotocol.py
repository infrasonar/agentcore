import asyncio
import logging
import time
from typing import Optional
from ..net.package import Package
from ..net.protocol import Protocol
from ..state import State


class RappProtocol(Protocol):

    PROTO_RAPP_PING = 0x40  # None
    PROTO_RAPP_READ = 0x41  # None
    PROTO_RAPP_PUSH = 0x42  # {..}
    PROTO_RAPP_UPDATE = 0x43  # None
    PROTO_RAPP_LOG = 0x44  # {"name": "wmi-probe", "start": 0}

    PROTO_RAPP_RES = 0x50  # {...} / null
    PROTO_RAPP_NO_AC = 0x51  # null
    PROTO_RAPP_NO_CONNECTION = 0x52  # null
    PROTO_RAPP_BUSY = 0x53  # null
    PROTO_RAPP_ERR = 0x54  # {"reason": "..."}

    def __init__(self):
        super().__init__()
        self.probe_key: Optional[str] = None
        self.version: Optional[str] = None
        self.keepalive: Optional[asyncio.Future] = None

    def connection_made(self, transport: asyncio.Transport):  # type: ignore
        super().connection_made(transport)
        if State.rapp is None:
            logging.info('rapp connected')
            State.rapp = self
            self.keepalive = asyncio.ensure_future(self.keepalive_loop())
        else:
            logging.warning('rapp already connected')
            transport.abort()

    def connection_lost(self, exc: Optional[Exception]):
        logging.info('rapp connecion lost')
        super().connection_lost(exc)
        if State.rapp is self:
            try:
                self.keepalive.cancel()
            except Exception:
                pass
            self.keepalive = None
            State.rapp = None

    async def keepalive_loop(self):
        pkg = Package.make(self.PROTO_RAPP_READ, is_binary=False)
        while True:
            await asyncio.sleep(3)
            try:
                data = await self.request(pkg, timeout=10)
                logging.debug(data)
                logging.info('rapp keepalive')
            except asyncio.CancelledError:
                break
            except Exception as e:
                msg = str(e) or type(e).__name__
                logging.warning(f'error on ping rapp: {msg}')
                try:
                    self.transport.close()
                except Exception:
                    pass
                break

    def _on_rapp(self, pkg: Package):
        try:
            future = self._get_future(pkg)
            assert future is not None, 'missing future; possible timeout'
            data = pkg.read_data()
        except Exception as e:
            msg = str(e) or type(e).__name__
            future.set_result({
                'protocol': RappProtocol.PROTO_RAPP_ERR,
                'data': {'reason': msg}
            })
        else:
            future.set_result({
                'protocol': pkg.tp,
                'data': data
            })

    def on_package_received(self, pkg: Package, _map={
        PROTO_RAPP_RES: _on_rapp,
        PROTO_RAPP_BUSY: _on_rapp,
        PROTO_RAPP_ERR: _on_rapp,
    }):
        handle = _map.get(pkg.tp)
        if handle is None:
            logging.error(f'unhandled package type: {pkg.tp}')
            self.close()
        else:
            handle(self, pkg)

    def close(self):
        if self.transport:
            self.transport.close()
