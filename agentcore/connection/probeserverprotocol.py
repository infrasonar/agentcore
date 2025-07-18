import asyncio
import logging
import time
from typing import Optional
from ..net.package import Package
from ..net.protocol import Protocol
from ..state import State


class ProbeServerProtocol(Protocol):

    PROTO_FAF_DUMP = 0x00

    PROTO_REQ_ANNOUNCE = 0x01

    PROTO_FAF_SET_ASSETS = 0x02  # Overwrites all assets

    PROTO_REQ_INFO = 0x03

    PROTO_FAF_UPSERT_ASSET = 0x04  # Overwrite/Add a single asset

    PROTO_FAF_UNSET_ASSETS = 0x05  # Remove given assets

    PROTO_REQ_UPLOAD_FILE = 0x07

    PROTO_REQ_DOWNLOAD_FILE = 0x08

    PROTO_RES_ANNOUNCE = 0x81

    PROTO_RES_INFO = 0x82

    PROTO_RES_ERR = 0xe0

    PROTO_RES_UPLOAD_FILE = 0xe3

    PROTO_RES_DOWNLOAD_FILE = 0xe4

    def __init__(self):
        super().__init__()
        self.probe_key: Optional[str] = None
        self.version: Optional[str] = None

    def connection_lost(self, exc: Optional[Exception]):
        logging.info(f'Connection lost; probe collector: `{self.probe_key}`')
        super().connection_lost(exc)
        try:
            State.probe_connections.remove(self)
        except KeyError:
            pass

    async def on_heartbeat(self):
        pkg = Package.make(ProbeServerProtocol.PROTO_REQ_INFO)
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

    def _on_upload_file(self, pkg: Package):
        asyncio.ensure_future(self._upload_file(pkg))

    async def _upload_file(self, pkg: Package):
        data = pkg.read_data()
        try:
            resp = await State.upload_file(data)
        except Exception as e:
            msg = str(e) or type(e).__name__
            resp_pkg = Package.make(
                ProbeServerProtocol.PROTO_RES_ERR,
                data=msg,
                pid=pkg.pid)
        else:
            resp_pkg = Package.make(
                ProbeServerProtocol.PROTO_RES_UPLOAD_FILE,
                data=resp,
                pid=pkg.pid)

        if self.transport:
            self.transport.write(resp_pkg.to_bytes())

    def _on_download_file(self, pkg: Package):
        asyncio.ensure_future(self._download_file(pkg))

    async def _download_file(self, pkg: Package):
        data = pkg.read_data()
        try:
            resp = await State.download_file(data)
        except Exception as e:
            msg = str(e) or type(e).__name__
            resp_pkg = Package.make(
                ProbeServerProtocol.PROTO_RES_ERR,
                data=msg,
                pid=pkg.pid)
        else:
            resp_pkg = Package.make(
                ProbeServerProtocol.PROTO_RES_DOWNLOAD_FILE,
                data=resp,
                pid=pkg.pid)

        if self.transport:
            self.transport.write(resp_pkg.to_bytes())

    def on_package_received(self, pkg: Package, _map={
        PROTO_FAF_DUMP: _on_faf_dump,
        PROTO_REQ_ANNOUNCE: _on_req_announce,
        PROTO_RES_INFO: _on_res_info,
        PROTO_REQ_UPLOAD_FILE: _on_upload_file,
        PROTO_REQ_DOWNLOAD_FILE: _on_download_file,
    }):
        handle = _map.get(pkg.tp)
        if handle is None:
            logging.error(f'unhandled package type: {pkg.tp}')
        else:
            handle(self, pkg)

    def close(self):
        if self.transport:
            self.transport.close()
