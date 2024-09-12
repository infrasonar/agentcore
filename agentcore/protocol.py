import asyncio
import logging
import time
from typing import Optional
from .net.package import Package
from .net.protocol import Protocol
from .state import State
from .version import __version__


class RespException(Exception):
    pass


class HubProtocol(Protocol):

    PROTO_REQ_DATA = 0x00

    PROTO_REQ_ANNOUNCE = 0x01

    PROTO_FAF_SET_ASSETS = 0x02  # Overwites all assets

    PROTO_REQ_INFO = 0x03

    PROTO_FAF_UPSERT_ASSET = 0x04  # Overwite/Add a single asset

    PROTO_FAF_UNSET_ASSETS = 0x05  # Remove given assets

    PROTO_RES_ANNOUNCE = 0x81

    PROTO_RES_INFO = 0x82

    PROTO_RES_ERR = 0xe0

    PROTO_RES_OK = 0xe1

    def __init__(self):
        super().__init__()

    def connection_lost(self, exc: Optional[Exception]):
        super().connection_lost(exc)

    def _on_res_announce(self, pkg: Package):
        try:
            agentcore_id, agentcores, assets = pkg.read_data()
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(f'ac announce failed: {msg}')
            return
        logging.info(
            'ac announce <'
            f'agentcore_id: {agentcore_id} '
            f'num assets: {len(assets)} '
            f'num agentcores: {len(agentcores)}>')
        State.agentcore_id = agentcore_id
        State.set_zones(agentcores)
        State.set_assets(assets)

        future = self._get_future(pkg)
        if future is None:
            return
        future.set_result(None)

    def _on_faf_set_assets(self, pkg: Package):
        try:
            agentcores, assets = pkg.read_data()
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(f'ac set assets failed: {msg}')
            return
        logging.info(f'ac set assets <num assets: {len(assets)}>')
        State.set_zones(agentcores)
        State.set_assets(assets)

    def _on_req_info(self, pkg: Package):
        asyncio.ensure_future(self._req_info(pkg))

    def _on_faf_upsert_asset(self, pkg: Package):
        try:
            asset = pkg.read_data()
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(f'ac upsert asset failed: {msg}')
            return

        logging.info(f'ac upsert asset id {asset[0]}')
        State.upsert_asset(asset)

    def _on_faf_unset_assets(self, pkg: Package):
        try:
            asset_ids = pkg.read_data()
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(f'ac upsert asset failed: {msg}')
            return

        logging.info(f'ac unset assets <num assets: {len(asset_ids)}>')
        State.unset_assets(asset_ids)

    async def _req_info(self, pkg: Package):
        logging.debug('ac heartbeat')

        probes = await asyncio.gather(*[
            conn.on_heartbeat()
            for conn in State.probe_connections
        ])

        having = set([p['key'] for p in probes])
        missing = State.required_probes() - having

        resp_pkg = Package.make(
            HubProtocol.PROTO_RES_INFO,
            pid=pkg.pid,
            data={
                'missing': tuple(missing),
                'probes': probes,
                'timestamp': time.time(),
                'version': __version__
            }
        )
        assert self.transport is not None
        self.transport.write(resp_pkg.to_bytes())

    def _on_res_err(self, pkg: Package):
        future = self._get_future(pkg)
        if future is None:
            return
        try:
            msg = pkg.read_data()
        except Exception as e:
            msg = str(e) or type(e).__name__
        future.set_exception(RespException(msg))

    def _on_res_ok(self, pkg):
        future = self._get_future(pkg)
        if future is None:
            return
        future.set_result(None)

    def on_package_received(self, pkg: Package, _map={
        PROTO_RES_ANNOUNCE: _on_res_announce,
        PROTO_FAF_SET_ASSETS: _on_faf_set_assets,
        PROTO_REQ_INFO: _on_req_info,
        PROTO_FAF_UPSERT_ASSET: _on_faf_upsert_asset,
        PROTO_FAF_UNSET_ASSETS: _on_faf_unset_assets,
        PROTO_RES_ERR: _on_res_err,
        PROTO_RES_OK: _on_res_ok,
    }):
        handle = _map.get(pkg.tp)
        if handle is None:
            logging.error(f'unhandled package type: {pkg.tp}')
        else:
            handle(self, pkg)
