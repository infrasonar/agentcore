import asyncio
import logging
import time
from ..net.package import Package
from ..net.protocol import Protocol
from ..state import State


class ProbeServerProtocol(Protocol):

    PROTO_FAF_DUMP = 0x00

    PROTO_REQ_ANNOUNCE = 0x01

    PROTO_FAF_SET_ASSETS = 0x02  # Overwites all assets

    PROTO_REQ_INFO = 0x03

    PROTO_FAF_UPSERT_ASSET = 0x04  # Overwite/Add a single asset

    PROTO_FAF_UNSET_ASSETS = 0x05  # Remove given assets

    PROTO_RES_ANNOUNCE = 0x81

    PROTO_RES_INFO = 0x82

    PROTO_RES_ERR = 0xe0

    def __init__(self):
        super().__init__()
        self.probe_name = None
        self.version = None

    async def on_heartbeat(self):
        pkg = Package.make(ProbeServerProtocol.PROTO_REQ_INFO)
        t0 = time.time()
        probe_timestamp = await self.request(pkg, timeout=10)
        return {
            'name': self.probe_name,
            'version': self.version,
            'timestamp': probe_timestamp,
            'roundtrip': time.time() - t0,
        }

    def send_unset_assets(self, asset_ids: list):
        resp_pkg = Package.make(
            ProbeServerProtocol.PROTO_FAF_UNSET_ASSETS, data=asset_ids)
        self.transport.write(resp_pkg.to_bytes())

    def send_upsert_asset(self, asset: list):
        resp_pkg = Package.make(
            ProbeServerProtocol.PROTO_FAF_UPSERT_ASSET, data=asset)
        self.transport.write(resp_pkg.to_bytes())

    def send_set_assets(self, assets: list):
        resp_pkg = Package.make(
            ProbeServerProtocol.PROTO_FAF_SET_ASSETS, data=assets)
        self.transport.write(resp_pkg.to_bytes())

    def _on_faf_dump(self, pkg):
        try:
            State.agentcore.queue.put_nowait(pkg)
        except asyncio.QueueFull:
            logging.error('hub queue full')

    def _on_req_announce(self, pkg: Package):
        probe_name, version = pkg.read_data()

        # TODOK
        assert not any(
            conn.probe_name == probe_name for conn in State.probe_connections)

        assets = State.probe_assets[probe_name]
        resp_pkg = Package.make(
            ProbeServerProtocol.PROTO_RES_ANNOUNCE, pid=pkg.pid, data=assets)
        self.transport.write(resp_pkg.to_bytes())

        self.probe_name = probe_name
        self.version = version
        State.probe_connections.add(self)

    def _on_res_info(self, pkg: Package):
        future = self._get_future(pkg)
        if future is None:
            return
        future.set_result(pkg.read_data())

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
