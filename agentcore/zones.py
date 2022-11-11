from typing import Tuple


class Zones:
    def __init__(
            self,
            agentcore_id: int,
            zone: int,
            agentcores: Tuple[Tuple[int, int]]):
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
        self._zone_idx = zone_ids.index(agentcore_id)
        self._zone_mod = len(zone_ids)
        self._all_idx = all_ids.index(agentcore_id)
        self._all_mod = len(all_ids)
        self._zones = zones

    def has_asset(self, asset_id: int, asset_zone: int) -> bool:
        if asset_zone == self._zone:
            return asset_id % self._zone_mod == self._zone_idx
        if asset_zone not in self._zones:
            return asset_id % self._all_mod == self._all_idx
        return False
