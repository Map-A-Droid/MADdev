import json
from typing import Tuple, List, Dict, Optional, Set, Any

from sqlalchemy.ext.asyncio import AsyncSession

from mapadroid.db.helper.GymHelper import GymHelper
from mapadroid.db.helper.PokemonHelper import PokemonHelper
from mapadroid.db.helper.PokestopHelper import PokestopHelper
from mapadroid.db.helper.RaidHelper import RaidHelper
from mapadroid.db.helper.WeatherHelper import WeatherHelper
from mapadroid.db.model import Raid, Gym, GymDetail, Weather, TrsQuest, Pokestop, Pokemon, TrsSpawn, PokemonDisplay, \
    PokestopIncident
from mapadroid.utils.WebhookJsonEncoder import WebhookJsonEncoder
from mapadroid.utils.logging import get_logger, LoggerEnums
from mapadroid.utils.madGlobals import MonSeenTypes

logger = get_logger(LoggerEnums.webhook)


class DbWebhookReader:
    @staticmethod
    async def get_raids_changed_since(session: AsyncSession, _timestamp: int):
        logger.debug2("DbWebhookReader::get_raids_changed_since called")
        # TODO: Consider geofences?
        raids_changed: List[Tuple[Raid, GymDetail, Gym]] = await RaidHelper.get_raids_changed_since(session,
                                                                                                    _timestamp=_timestamp)

        ret = []
        for (raid, gym_detail, gym) in raids_changed:
            ret.append({
                "gym_id": raid.gym_id,
                "level": raid.level,
                "spawn": int(raid.spawn.timestamp()),
                "start": int(raid.start.timestamp()),
                "end": int(raid.end.timestamp()),
                "pokemon_id": raid.pokemon_id,
                "cp": raid.cp,
                "move_1": raid.move_1,
                "move_2": raid.move_2,
                "last_scanned": int(raid.last_scanned.timestamp()),
                "form": raid.form,
                "name": gym_detail.name,
                "url": gym_detail.url,
                "latitude": gym.latitude,
                "longitude": gym.longitude,
                "team_id": gym.team_id,
                "weather_boosted_condition": gym.weather_boosted_condition,
                "is_exclusive": raid.is_exclusive,
                "gender": raid.gender,
                "is_ex_raid_eligible": gym.is_ex_raid_eligible,
                "costume": raid.costume,
                "evolution": raid.evolution
            })
        return ret

    @staticmethod
    async def get_weather_changed_since(session: AsyncSession, _timestamp: int):
        logger.debug2("DbWebhookReader::get_weather_changed_since called")
        weather_changed: List[Weather] = await WeatherHelper.get_changed_since(session, _timestamp=_timestamp)

        ret = []
        for weather in weather_changed:
            ret.append({
                "s2_cell_id": weather.s2_cell_id,
                "latitude": weather.latitude,
                "longitude": weather.longitude,
                "cloud_level": weather.cloud_level,
                "rain_level": weather.rain_level,
                "wind_level": weather.wind_level,
                "snow_level": weather.snow_level,
                "fog_level": weather.fog_level,
                "wind_direction": weather.wind_direction,
                "gameplay_weather": weather.gameplay_weather,
                "severity": weather.severity,
                "warn_weather": weather.warn_weather,
                "world_time": weather.world_time,
                "last_updated": int(weather.last_updated.timestamp())
            })
        return ret

    @staticmethod
    async def get_quests_changed_since(session: AsyncSession, _timestamp: int) -> Dict[int, Tuple[Pokestop,
                                                                                                  Dict[int, TrsQuest]]]:
        logger.debug2("DbWebhookReader::get_quests_changed_since called")
        quests_with_changes: Dict[int, Tuple[Pokestop, Dict[int, TrsQuest]]] = await PokestopHelper.get_with_quests(
            session, timestamp=_timestamp)
        return quests_with_changes

    @staticmethod
    async def get_gyms_changed_since(session: AsyncSession, _timestamp: int):
        logger.debug2("DbWebhookReader::get_gyms_changed_since called")
        gyms_changed: List[Tuple[Gym, GymDetail]] = await GymHelper.get_changed_since(session, _timestamp)

        ret = []
        for (gym, gym_detail) in gyms_changed:
            ret.append({
                "gym_id": gym.gym_id,
                "team_id": gym.team_id,
                "guard_pokemon_id": gym.guard_pokemon_id,
                "slots_available": gym.slots_available,
                "latitude": gym.latitude,
                "longitude": gym.longitude,
                "total_cp": gym.total_cp,
                "is_in_battle": gym.is_in_battle,
                "weather_boosted_condition": gym.weather_boosted_condition,
                "last_scanned": int(gym.last_scanned.timestamp()),
                "last_modified": int(gym.last_modified.timestamp()),
                "name": gym_detail.name,
                "url": gym_detail.url,
                "description": gym_detail.description,
                "is_ex_raid_eligible": gym.is_ex_raid_eligible,
                "is_ar_scan_eligible": gym.is_ar_scan_eligible
            })
        return ret

    @staticmethod
    async def get_stops_changed_since(session: AsyncSession, _timestamp: int) -> List[Dict[str, Any]]:
        logger.debug2("DbWebhookReader::get_stops_changed_since called")
        stops_with_changes: Dict[Pokestop, List[PokestopIncident]] = await PokestopHelper\
            .get_changed_since_or_incidents(session, _timestamp)
        ret: List[Dict[str, Any]] = []
        for stop, incidents in stops_with_changes.items():
            stop_entry: Dict[str, Any] = {
                'pokestop_id': stop.pokestop_id,
                'latitude': stop.latitude,
                'longitude': stop.longitude,
                'lure_expiration': int(stop.lure_expiration.timestamp()) if stop.lure_expiration is not None else None,
                'name': stop.name,
                'image': stop.image,
                'active_fort_modifier': stop.active_fort_modifier,
                "last_modified": int(stop.last_modified.timestamp()) if stop.last_modified is not None else None,
                "last_updated": int(stop.last_updated.timestamp()) if stop.last_updated is not None else None,
            }

            if incidents:
                # backwards compatibility...
                first_incident = incidents[0]
                stop_entry["incident_start"] = int(first_incident.incident_start.timestamp()) \
                    if first_incident.incident_start is not None else None
                stop_entry["incident_expiration"] = int(first_incident.incident_expiration.timestamp()) \
                    if first_incident.incident_expiration is not None else None
                stop_entry["incident_grunt_type"] = first_incident.character_display
                stop_entry["incident_display_type"] = first_incident.incident_display_type
            stop_entry["incidents"] = incidents

            ret.append(stop_entry)
        return ret

    @staticmethod
    async def get_mon_changed_since(session: AsyncSession, _timestamp: int,
                                    mon_types: Optional[Set[MonSeenTypes]] = None):
        logger.debug2("DbWebhookReader::get_mon_changed_since called")
        mons_with_changes: List[
            Tuple[Pokemon, TrsSpawn, Optional[Pokestop], Optional[
                PokemonDisplay]]] = await PokemonHelper.get_changed_since(
            session,
            _timestamp,
            mon_types)
        ret = []
        for (mon, spawn, stop, mon_display) in mons_with_changes:
            if mon.latitude == 0 and mon.seen_type == MonSeenTypes.lure_encounter.value:
                continue
            ret.append({
                "encounter_id": mon.encounter_id,
                "pokemon_id": mon.pokemon_id,
                "last_modified": mon.last_modified,
                "spawnpoint_id": mon.spawnpoint_id,
                "latitude": mon.latitude,
                "longitude": mon.longitude,
                "disappear_time": int(mon.disappear_time.timestamp()),
                "individual_attack": mon.individual_attack,
                "individual_defense": mon.individual_defense,
                "individual_stamina": mon.individual_stamina,
                "move_1": mon.move_1,
                "move_2": mon.move_2,
                "cp": mon.cp,
                "cp_multiplier": mon.cp_multiplier,
                "gender": mon.gender,
                "form": mon.form,
                "costume": mon.costume,
                "height": mon.height,
                "weight": mon.weight,
                "weather_boosted_condition": mon.weather_boosted_condition,
                "base_catch": mon.catch_prob_1,
                "great_catch": mon.catch_prob_2,
                "ultra_catch": mon.catch_prob_3,
                "spawn_verified": True if spawn and spawn.calc_endminsec else False,
                "fort_id": mon.fort_id,
                "stop_name": stop.name if stop else None,
                "stop_url": stop.image if stop else None,
                "cell_id": mon.cell_id,
                "seen_type": mon.seen_type,
                "display_pokemon": mon_display.pokemon if mon_display else None,
                "display_form": mon_display.form if mon_display else None,
                "display_costume": mon_display.costume if mon_display else None,
                "display_gender": mon_display.gender if mon_display else None,
                "size": mon.size
            })
        return ret
