import logging
from typing import Tuple, List


class Zones:
    def __init__(
            self,
            agentcore_id: int,
            zone: int,
            agentcores: List[Tuple[int, int]]):
        agentcores = sorted(agentcores)
        zone_ids = []
        all_ids = []
        zones = set()
        for ac in agentcores:
            ac_id, ac_zone = ac
            if ac_zone == zone:
                zone_ids.append(ac_id)
            else:
                zones.add(ac_zone)
            all_ids.append(ac_id)

        self._zone = zone
        self._zones = zones

        self._zone_mod = len(zone_ids)
        self._all_mod = len(all_ids)

        try:
            self._zone_idx = zone_ids.index(agentcore_id)
            self._all_idx = all_ids.index(agentcore_id)
        except ValueError:
            logging.error(
                f'failed to find a zone for agent core Id {agentcore_id}; '
                'please check if the agentcore exists')
            # zone_idx and all_idx as None forces `has_asset` to return False
            self._zone_idx = None
            self._all_idx = None

    def has_asset(self, asset_id: int, asset_zone: int) -> bool:
        if asset_zone == self._zone:
            return asset_id % self._zone_mod == self._zone_idx
        if asset_zone not in self._zones:
            return asset_id % self._all_mod == self._all_idx
        return False
