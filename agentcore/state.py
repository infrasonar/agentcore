from __future__ import annotations
import msgpack
import logging
import os
from typing import Optional, Tuple, List, Set, Dict, TYPE_CHECKING, Any
from collections import defaultdict
from .zones import Zones
if TYPE_CHECKING:
    from .connection.probeserverprotocol import ProbeServerProtocol
    from .connection.rappprotocol import RappProtocol
    from .client import Agentcore


PATH_IDX, NAMES_IDX, CONFIG_IDX = range(3)
ASSET_ID, ZONE, CHECK_ID = range(3)


class State:
    agentcore: Optional[Agentcore] = None
    probe_connections: Set[ProbeServerProtocol] = set()
    probe_assets: Dict[str, list] = defaultdict(list)
    rapp: Optional[RappProtocol] = None
    zone: int = 0
    name: str
    token: str
    agentcore_id: Optional[int] = None  # from JSON/announce
    zones: Optional[Zones] = None  # after announce
    assets_fn: Optional[str] = None

    @classmethod
    def set_zones(cls, agentcores: List[Tuple[int, int]]):
        assert cls.agentcore_id is not None  # is set after announce
        cls.zones = Zones(cls.agentcore_id, cls.zone, agentcores)

    @classmethod
    def unset_assets(cls, asset_ids: list):
        set_asset_ids = set(asset_ids)

        # cleanup all assets
        for assets in cls.probe_assets.values():
            for i, check in reversed(list(enumerate(assets))):
                if check[PATH_IDX][ASSET_ID] in set_asset_ids:
                    del assets[i]

        for conn in cls.probe_connections:
            conn.send_unset_assets(asset_ids)

    @classmethod
    def upsert_asset(cls, asset: list):
        """Update or add a single asset."""
        assert cls.zones is not None  # is set after announce
        asset_id, asset_zone, asset_name, probes = asset

        # first remove all checks for the current asset
        for assets in cls.probe_assets.values():
            for i, check in reversed(list(enumerate(assets))):
                if check[PATH_IDX][ASSET_ID] == asset_id:
                    del assets[i]

        if not cls.zones.has_asset(asset_id, asset_zone):
            for conn in cls.probe_connections:
                conn.send_unset_assets([asset_id])
            return

        new = defaultdict(list)
        for probe_key, probe_config, checks_ in probes:
            for check_id, check_key, interval, check_config in checks_:
                new[probe_key].append([
                    [asset_id, check_id],
                    [asset_name, check_key],
                    {
                        '_interval': interval,
                        **(probe_config or {}),  # can be empty
                        **(check_config or {}),  # can be empty
                    },
                ])

        for probe_key, checks in new.items():
            cls.probe_assets[probe_key].extend(checks)

        for conn in cls.probe_connections:
            conn.send_upsert_asset([asset_id, new[conn.probe_key]])

    @classmethod
    def set_assets(cls, assets: list):
        """Overwrites all the assets."""
        assert cls.zones is not None  # is set after announce
        new = defaultdict(list)
        for asset_id, asset_zone, asset_name, probes in assets:
            if not cls.zones.has_asset(asset_id, asset_zone):
                continue
            for probe_key, probe_config, checks_ in probes:
                for check_id, check_key, interval, check_config in checks_:
                    new[probe_key].append([
                        [asset_id, check_id],
                        [asset_name, check_key],
                        {
                            '_interval': interval,
                            **(probe_config or {}),  # can be empty
                            **(check_config or {}),  # can be empty
                        },
                    ])
        cls.probe_assets = new

        for conn in cls.probe_connections:
            conn.send_set_assets(new[conn.probe_key])

    @classmethod
    def dump_probe_assets(cls):
        if cls.assets_fn is None:
            logging.debug('dump file still None')
            return

        logging.info(f'write assets to: {cls.assets_fn}')
        try:
            with open(cls.assets_fn, 'wb') as fp:
                msgpack.pack(cls.probe_assets, fp)
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(f'failed to write: {cls.assets_fn} ({msg})')

    @classmethod
    def required_probes(cls) -> Set[str]:
        return set(key for key, items in cls.probe_assets.items() if items)

    @classmethod
    def load_probe_assets(cls):
        assert cls.assets_fn is not None  # is set after connect
        logging.warning(f'load assets from: {cls.assets_fn}')
        try:
            with open(cls.assets_fn, 'rb') as fp:
                data: dict = msgpack.unpack(fp)  # type: ignore
                for k, v in data.items():
                    cls.probe_assets[k] = v
        except Exception as e:
            msg = str(e) or type(e).__name__
            logging.error(f'failed to read: {cls.assets_fn} ({msg})')
        else:
            for conn in cls.probe_connections:
                if conn.probe_key is not None and \
                        conn.probe_key in cls.probe_assets:
                    conn.send_set_assets(cls.probe_assets[conn.probe_key])

    @classmethod
    def remove_assets_fn(cls):
        assert cls.assets_fn is not None  # is set directly after connect
        if os.path.exists(cls.assets_fn):
            try:
                os.remove(cls.assets_fn)
            except Exception as e:
                msg = str(e) or type(e).__name__
                logging.error(f'failed to remove: {cls.assets_fn} ({msg})')
            else:
                logging.info(f'removed assets file: {cls.assets_fn}')

    @classmethod
    def stop(cls):
        for conn in cls.probe_connections:
            conn.close()
        assert cls.agentcore is not None
        cls.agentcore.close()
        cls.dump_probe_assets()

    @classmethod
    async def upload_file(cls, data: Any):
        assert cls.agentcore
        resp = await cls.agentcore.upload_file(data)
        return resp

    @classmethod
    async def download_file(cls, data: Any):
        assert cls.agentcore
        resp = await cls.agentcore.download_file(data)
        return resp
