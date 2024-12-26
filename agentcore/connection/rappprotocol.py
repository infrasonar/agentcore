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

    def connection_made(self, transport: asyncio.Transport):  # type: ignore
        super().connection_made(transport)



    def connection_lost(self, exc: Optional[Exception]):
        logging.info(f'Connecion lost; probe collector: `{self.probe_key}`')
        super().connection_lost(exc)
        try:
            State.probe_connections.remove(self)
        except KeyError:
            pass

    async def keepalive(self):
        while True:
            pkg = Package.make(self.PROTO_RAPP_PING, is_binary=True)
        t0 = time.time()
        try:
            probe_timestamp = await self.request(pkg, timeout=10)
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(msg)
            probe_timestamp = 1  # don't want the heartbeat to fail

        return {
            'key': self.probe_key,
            'version': self.version,
            'timestamp': probe_timestamp,
            'roundtrip': time.time() - t0,
        }

    def send_unset_assets(self, asset_ids: list):
        assert self.transport is not None
        resp_pkg = Package.make(
            ProbeServerProtocol.PROTO_FAF_UNSET_ASSETS, data=asset_ids)
        self.transport.write(resp_pkg.to_bytes())

    def send_upsert_asset(self, asset: list):
        assert self.transport is not None
        resp_pkg = Package.make(
            ProbeServerProtocol.PROTO_FAF_UPSERT_ASSET, data=asset)
        self.transport.write(resp_pkg.to_bytes())

    def send_set_assets(self, assets: list):
        assert self.transport is not None
        resp_pkg = Package.make(
            ProbeServerProtocol.PROTO_FAF_SET_ASSETS, data=assets)
        self.transport.write(resp_pkg.to_bytes())

    def _on_faf_dump(self, pkg):
        assert State.agentcore is not None
        try:
            State.agentcore.queue.put_nowait(pkg)
        except asyncio.QueueFull:
            logging.warning('hub queue full; drop first in queue')
            try:
                State.agentcore.queue.get_nowait()
                State.agentcore.queue.put_nowait(pkg)
            except Exception as e:
                msg = str(e) or type(e).__name__
                logging.error(f'failed to add package to hub queue: {msg}')

    def _on_req_announce(self, pkg: Package):
        assert self.transport is not None
        try:
            try:
                name, version = pkg.read_data()
            except Exception as e:
                msg = str(e) or type(e).__name__
                raise Exception(f'unpack announce response failed: {msg}')

            logging.info(f'probe collector announce: {name} v{version}')

            for conn in State.probe_connections:
                if conn.probe_key == name:
                    raise Exception(
                        'got a double probe collector announcement: '
                        f'{name} v{conn.version}; close the connection')

            assets = State.probe_assets.get(name)
            if assets is None:
                logging.warning(
                    f'no assets found for probe collector: {name}')
                assets = []

            resp_pkg = Package.make(
                ProbeServerProtocol.PROTO_RES_ANNOUNCE,
                pid=pkg.pid,
                data=assets)

            try:
                self.transport.write(resp_pkg.to_bytes())
            except Exception as e:
                msg = str(e) or type(e).__name__
                raise Exception(f'failed to write announce response: {msg}')

            self.probe_key = name
            self.version = version
            State.probe_connections.add(self)

        except Exception as e:
            logging.error(f'{e}; close the connection')
            try:
                self.transport.close()
            except Exception as e:
                msg = str(e) or type(e).__name__
                logging.error(f'attempt to close connection has failed: {msg}')

    def _on_res_info(self, pkg: Package):
        future = self._get_future(pkg)
        if future is None:
            return
        try:
            data = pkg.read_data()
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(data)

    def on_package_received(self, pkg: Package, _map={
        PROTO_FAF_DUMP: _on_faf_dump,
        PROTO_REQ_ANNOUNCE: _on_req_announce,
        PROTO_RES_INFO: _on_res_info,
    }):
        handle = _map.get(pkg.tp)
        if handle is None:
            logging.error(f'unhandled package type: {pkg.tp}')
        else:
            handle(self, pkg)

    def close(self):
        if self.transport:
            self.transport.close()
