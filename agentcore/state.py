from __future__ import annotations
from typing import Set, TYPE_CHECKING
from collections import defaultdict
from weakref import WeakSet
if TYPE_CHECKING:
    from .connection.probeserverprotocol import ProbeServerProtocol


PATH_IDX, NAMES_IDX, CONFIG_IDX = range(3)
CONTAINER_ID, ASSET_ID, CHECK_ID = range(3)


class State:
    hubclient = None
    probe_connections: Set[ProbeServerProtocol] = WeakSet()
    probe_assets = defaultdict(list)

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
        container_id, asset_id, asset_name, probes, checks = asset

        # first remove all checks for the current asset
        for assets in cls.probe_assets.values():
            for i, check in reversed(list(enumerate(assets))):
                if check[PATH_IDX][ASSET_ID] == asset_id:
                    del assets[i]

        new = defaultdict(list)
        for probe_name, probe_config, checks_ in probes:
            for check_id, check_name, interval, check_config in checks_:
                new[probe_name].append([
                    [container_id, asset_id, check_id],
                    [asset_name, check_name],
                    {
                        '_interval': interval,
                        **(probe_config or {}),  # can be empty
                        **(check_config or {}),  # can be empty
                    },
                ])

        for probe_name, checks in new.items():
            cls.probe_assets[probe_name].extend(checks)

        for conn in cls.probe_connections:
            conn.send_upsert_asset([asset_id, new[conn.probe_name]])

    @classmethod
    def set_assets(cls, assets: list):
        """Overwites all the assets."""
        new = defaultdict(list)
        for container_id, asset_id, asset_name, probes, checks in assets:
            checks = dict(checks)
            for probe_name, probe_config, checks_ in probes:
                for check_id, check_name, interval in checks_:
                    new[probe_name].append([
                        [container_id, asset_id, check_id],
                        [asset_name, check_name],
                        {
                            '_interval': interval,
                            **(probe_config or {}),  # can be empty
                            **(checks.get(check_id) or {}),  # can be empty
                        },
                    ])
        cls.probe_assets = new

        for conn in cls.probe_connections:
            conn.send_set_assets(new[conn.probe_name])

    @classmethod
    def stop(cls):
        for conn in cls.probe_connections:
            conn.close()

        cls.hubclient.close()
