import json
import math
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

import sqlalchemy
from bitstring import BitArray
from google.protobuf.internal.containers import (
    RepeatedCompositeFieldContainer, RepeatedScalarFieldContainer)
from redis import Redis
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import mapadroid.mitm_receiver.protos.Rpc_pb2 as pogoprotos
from mapadroid.db.helper.GymDetailHelper import GymDetailHelper
from mapadroid.db.helper.GymHelper import GymHelper
from mapadroid.db.helper.PokemonDisplayHelper import PokemonDisplayHelper
from mapadroid.db.helper.PokemonHelper import PokemonHelper
from mapadroid.db.helper.PokestopHelper import PokestopHelper
from mapadroid.db.helper.PokestopIncidentHelper import PokestopIncidentHelper
from mapadroid.db.helper.RaidHelper import RaidHelper
from mapadroid.db.helper.RouteHelper import RouteHelper
from mapadroid.db.helper.StationHelper import StationHelper
from mapadroid.db.helper.TrsEventHelper import TrsEventHelper
from mapadroid.db.helper.TrsQuestHelper import TrsQuestHelper
from mapadroid.db.helper.TrsS2CellHelper import TrsS2CellHelper
from mapadroid.db.helper.TrsSpawnHelper import TrsSpawnHelper
from mapadroid.db.helper.TrsStatsDetectSeenTypeHelper import \
    TrsStatsDetectSeenTypeHelper
from mapadroid.db.helper.WeatherHelper import WeatherHelper
from mapadroid.db.model import (Gym, GymDetail, Pokemon, Pokestop,
                                PokestopIncident, Raid, Route, Station, TrsEvent,
                                TrsQuest, TrsSpawn, TrsStatsDetectSeenType,
                                Weather)
from mapadroid.db.PooledQueryExecutor import PooledQueryExecutor
from mapadroid.mitm_receiver.protos.ProtoHelper import ProtoHelper
from mapadroid.utils.DatetimeWrapper import DatetimeWrapper
from mapadroid.utils.gamemechanicutil import (gen_despawn_timestamp,
                                              is_mon_ditto_raw)
from mapadroid.utils.logging import LoggerEnums, get_logger
from mapadroid.utils.madConstants import (REDIS_CACHETIME_CELLS,
                                          REDIS_CACHETIME_GYMS,
                                          REDIS_CACHETIME_MON_LURE_IV,
                                          REDIS_CACHETIME_POKESTOP_DATA,
                                          REDIS_CACHETIME_RAIDS,
                                          REDIS_CACHETIME_ROUTE,
                                          REDIS_CACHETIME_STATIONS,
                                          REDIS_CACHETIME_STOP_DETAILS,
                                          REDIS_CACHETIME_WEATHER)
from mapadroid.utils.madGlobals import MadGlobals, MonSeenTypes, QuestLayer
from mapadroid.utils.questGen import QuestGen
from mapadroid.utils.s2Helper import S2Helper

logger = get_logger(LoggerEnums.database)


class DbPogoProtoSubmitRaw:
    """
    Hosts all methods related to submitting protocol data to the database.
    """
    default_spawndef = 240
    _db_exec: PooledQueryExecutor
    _cache: Redis

    def __init__(self, db_exec: PooledQueryExecutor):
        self._db_exec: PooledQueryExecutor = db_exec

    async def setup(self):
        self._cache: Redis = await self._db_exec.get_cache()

    async def mons(self, session: AsyncSession, timestamp: float,
                   map_proto: pogoprotos.GetMapObjectsOutProto) -> List[int]:
        """
        Update/Insert mons from a GMO response

        Returns: List of encounterIDs of wild mons in GMO
        """
        logger.debug3("DbPogoProtoSubmit::mons called with data received")
        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = map_proto.map_cell
        encounter_ids_in_gmo: List[int] = []
        now = DatetimeWrapper.fromtimestamp(timestamp)
        if not cells:
            return encounter_ids_in_gmo
        for cell in cells:
            for wild_mon in cell.wild_pokemon:
                spawnid: int = int(str(wild_mon.spawn_point_id), 16)
                lat: float = wild_mon.latitude
                lon: float = wild_mon.longitude
                mon_id: int = wild_mon.pokemon.pokemon_id
                encounter_id: int = wild_mon.encounter_id

                if encounter_id < 0:
                    encounter_id = encounter_id + 2 ** 64
                encounter_ids_in_gmo.append(encounter_id)

                cache_key = "mon{}-{}".format(encounter_id, mon_id)
                if await self._cache.exists(cache_key):
                    continue

                # get known spawn end time and feed into despawn time calculation
                # getdetspawntime = self._get_detected_endtime(str(spawnid))
                spawnpoint: Optional[TrsSpawn] = await TrsSpawnHelper.get(session, spawnid)
                despawn_time_unix = gen_despawn_timestamp(spawnpoint.calc_endminsec if spawnpoint else None, timestamp,
                                                          MadGlobals.application_args.default_unknown_timeleft)
                despawn_time = DatetimeWrapper.fromtimestamp(despawn_time_unix)

                if spawnpoint is None:
                    logger.debug3("adding mon (#{}) at {}, {}. Despawns at {} (init) ({})", mon_id, lat, lon,
                                  despawn_time.strftime("%Y-%m-%d %H:%M:%S"), spawnid)
                else:
                    logger.debug3("adding mon (#{}) at {}, {}. Despawns at {} (non-init) ({})", mon_id, lat, lon,
                                  despawn_time.strftime("%Y-%m-%d %H:%M:%S"), spawnid)

                async with session.begin_nested() as nested_transaction:
                    mon: Optional[Pokemon] = await PokemonHelper.get(session, encounter_id)
                    if not mon:
                        mon: Pokemon = Pokemon()
                        mon.encounter_id = encounter_id
                        mon.spawnpoint_id = spawnid
                        mon.latitude = lat
                        mon.longitude = lon
                    mon.pokemon_id = mon_id
                    if mon.seen_type not in [MonSeenTypes.encounter.name, MonSeenTypes.lure_encounter.name]:
                        # TODO: Any other types not to overwrite?
                        mon.seen_type = MonSeenTypes.wild.name
                    mon_display: pogoprotos.PokemonDisplayProto = wild_mon.pokemon.pokemon_display
                    if mon_id == 132:
                        # handle ditto
                        mon.pokemon_id = 132
                        mon.gender = 3
                        mon.costume = 0
                        mon.form = 0
                    else:
                        mon.pokemon_id = mon_id
                        mon.gender = mon_display.gender.real
                        mon.costume = mon_display.costume.real
                        mon.form = mon_display.form.real

                    # TODO handle weather boost condition changes for redoing IV+ditto (set ivs to null again)
                    #  Further we should probably reset IVs if pokemon_id changes as well

                    mon.disappear_time = despawn_time
                    # TODO: weather_boosted_value in json...
                    mon.weather_boosted_condition = mon_display.weather_boosted_condition.real
                    mon.last_modified = now
                    try:
                        session.add(mon)
                        await nested_transaction.commit()
                        cache_time = int(despawn_time_unix - int(DatetimeWrapper.now().timestamp()))
                        if cache_time > 0:
                            await self._cache.set(cache_key, 1, ex=cache_time)
                    except sqlalchemy.exc.IntegrityError as e:
                        logger.debug("Failed committing mon {} ({}). Safe to ignore.", encounter_id, str(e))
                        await nested_transaction.rollback()
                        continue
                await session.commit()
        return encounter_ids_in_gmo

    async def mons_nearby(self, session: AsyncSession, timestamp: float,
                          map_proto: pogoprotos.GetMapObjectsOutProto) -> Tuple[List[int], List[int]]:
        """
        Insert nearby mons
        """
        stop_encounters: List[int] = []
        cell_encounters: List[int] = []
        logger.debug3("DbPogoProtoSubmit::nearby_mons called with data received")
        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = map_proto.map_cell
        if not cells:
            return cell_encounters, stop_encounters

        for cell in cells:
            cell_id: int = cell.s2_cell_id
            nearby_mons: RepeatedCompositeFieldContainer[pogoprotos.NearbyPokemonProto] = cell.nearby_pokemon
            for nearby_mon in nearby_mons:
                display: pogoprotos.PokemonDisplayProto = nearby_mon.pokemon_display
                weather_boosted: int = display.weather_boosted_condition.real
                mon_id: int = nearby_mon.pokedex_number
                encounter_id: int = nearby_mon.encounter_id
                if encounter_id < 0:
                    encounter_id = encounter_id + 2 ** 64

                cache_key: str = "monnear{}-{}".format(encounter_id, mon_id)
                encounter_key: str = "moniv{}-{}-{}".format(encounter_id, weather_boosted, mon_id)
                wild_key: str = "mon{}-{}".format(encounter_id, mon_id)
                if (await self._cache.exists(wild_key) or await self._cache.exists(encounter_key)
                        or await self._cache.exists(cache_key)):
                    continue
                stop_id: Optional[str] = nearby_mon.fort_id
                form: int = display.form.real
                costume: int = display.costume.real
                gender: int = display.gender.real

                now: datetime = DatetimeWrapper.fromtimestamp(timestamp)
                disappear_time: datetime = now + timedelta(minutes=MadGlobals.application_args.default_nearby_timeleft)

                if not MadGlobals.application_args.disable_nearby_cell and not stop_id and cell_id:
                    lat, lon, _ = S2Helper.get_position_from_cell(cell_id)
                    stop_id = None
                    db_cell = cell_id
                    seen_type: MonSeenTypes = MonSeenTypes.nearby_cell
                    # TODO: Move above cache check...
                    cell_encounters.append(encounter_id)
                else:
                    db_cell = None
                    seen_type: MonSeenTypes = MonSeenTypes.nearby_stop
                    fort: Optional[Union[Pokestop, Gym]] = await PokestopHelper.get(session, stop_id)
                    if not fort:
                        fort: Optional[Gym] = await GymHelper.get(session, stop_id)

                    if fort:
                        lat, lon = fort.latitude, fort.longitude
                    else:
                        lat, lon = (0, 0)
                    # TODO: Move above cache check...
                    stop_encounters.append(encounter_id)

                spawnpoint = 0
                try:
                    async with session.begin_nested() as nested_transaction:
                        mon: Optional[Pokemon] = await PokemonHelper.get(session, encounter_id)
                        if not mon:
                            mon: Pokemon = Pokemon()
                            mon.encounter_id = encounter_id
                            mon.spawnpoint_id = spawnpoint
                            mon.latitude = lat
                            mon.longitude = lon
                            mon.cell_id = db_cell
                            mon.fort_id = stop_id
                            mon.seen_type = seen_type.name
                            mon.disappear_time = disappear_time

                        if mon_id == 132:
                            # handle ditto
                            mon.pokemon_id = 132
                            mon.gender = 3
                            mon.costume = 0
                            mon.form = 0
                        else:
                            mon.pokemon_id = mon_id
                            mon.gender = gender
                            mon.costume = costume
                            mon.form = form
                        mon.weather_boosted_condition = weather_boosted
                        mon.last_modified = now
                        session.add(mon)
                        await nested_transaction.commit()
                        await self._cache.set(cache_key, 1, ex=MadGlobals.application_args.default_nearby_timeleft * 60)
                except sqlalchemy.exc.IntegrityError as e:
                    logger.debug("Failed committing nearby mon {} ({}). Safe to ignore.", encounter_id, str(e))
                    # await nested_transaction.rollback()
                    continue
        return cell_encounters, stop_encounters

    async def mon_iv(self, session: AsyncSession, timestamp: float,
                     encounter_proto: pogoprotos.EncounterOutProto) -> Optional[Tuple[int, bool]]:
        """
        Update/Insert a mon with IVs
        """
        wild_pokemon: Optional[pogoprotos.WildPokemonProto] = encounter_proto.pokemon
        if wild_pokemon is None or wild_pokemon.encounter_id == 0 or not wild_pokemon.spawn_point_id:
            logger.warning("Encounter proto of no use (status: {}).", encounter_proto.status.real)
            return None
        encounter_id: int = wild_pokemon.encounter_id
        pokemon_data: pogoprotos.PokemonProto = wild_pokemon.pokemon
        mon_id: int = pokemon_data.pokemon_id
        pokemon_display: pogoprotos.PokemonDisplayProto = pokemon_data.pokemon_display
        weather_boosted: int = pokemon_display.weather_boosted_condition
        if encounter_id < 0:
            encounter_id = encounter_id + 2 ** 64
        cache_key: str = "moniv{}-{}-{}".format(encounter_id, weather_boosted, mon_id)
        if await self._cache.exists(cache_key):
            return None

        logger.debug3("Updating IV sent for encounter at {}", timestamp)

        spawnid: int = int(str(wild_pokemon.spawn_point_id), 16)
        spawnpoint: Optional[TrsSpawn] = await TrsSpawnHelper.get(session, spawnid)
        despawn_time_unix: int = gen_despawn_timestamp(spawnpoint.calc_endminsec if spawnpoint else None, timestamp,
                                                       MadGlobals.application_args.default_unknown_timeleft)
        despawn_time: datetime = DatetimeWrapper.fromtimestamp(despawn_time_unix)

        latitude: float = wild_pokemon.latitude
        longitude: float = wild_pokemon.longitude
        is_shiny: bool = pokemon_display.shiny
        if spawnpoint is None:
            logger.debug3("updating IV mon #{} at {}, {}. Despawning at {} (init)", encounter_id, latitude,
                          longitude, despawn_time)
        else:
            logger.debug3("updating IV mon #{} at {}, {}. Despawning at {} (non-init)", encounter_id,
                          latitude, longitude, despawn_time)

        capture_probability: pogoprotos.CaptureProbabilityProto = encounter_proto.capture_probability
        capture_probability_list: RepeatedScalarFieldContainer[float] = capture_probability.capture_probability

        # ditto detector
        form, gender, mon_id, move_1, move_2 = await self._extract_data_or_set_ditto(mon_id, pokemon_data,
                                                                                     pokemon_display)
        now = DatetimeWrapper.fromtimestamp(timestamp)
        time_start_submit = time.time()
        mon: Optional[Pokemon] = await PokemonHelper.get(session, encounter_id)
        if not mon:
            mon: Pokemon = Pokemon()
            mon.encounter_id = encounter_id
        mon.spawnpoint_id = spawnid
        mon.pokemon_id = mon_id
        mon.latitude = latitude
        mon.longitude = longitude
        mon.cell_id = None
        mon.seen_type = MonSeenTypes.encounter.name
        mon.disappear_time = despawn_time
        mon.individual_attack = pokemon_data.individual_attack
        mon.individual_defense = pokemon_data.individual_defense
        mon.individual_stamina = pokemon_data.individual_stamina
        mon.move_1 = move_1
        mon.move_2 = move_2
        mon.cp = pokemon_data.cp
        mon.cp_multiplier = pokemon_data.cp_multiplier
        mon.weight = pokemon_data.weight_kg
        mon.height = pokemon_data.height_m
        mon.gender = gender
        mon.catch_prob_1 = float(capture_probability_list[0])
        mon.catch_prob_2 = float(capture_probability_list[1])
        mon.catch_prob_3 = float(capture_probability_list[2])
        mon.rating_attack = mon.rating_defense = None
        mon.weather_boosted_condition = weather_boosted
        mon.costume = pokemon_display.costume
        mon.form = form
        mon.last_modified = now
        mon.size = pokemon_data.size
        logger.debug("Submitting IV {} scanned at {}", encounter_id, timestamp)
        session.add(mon)
        await self.maybe_save_ditto(session, pokemon_display, encounter_id, mon_id, pokemon_data)
        await session.commit()
        cache_time = int(despawn_time_unix - int(DatetimeWrapper.now().timestamp()))
        if cache_time > 0:
            await self._cache.set(cache_key, 1, ex=cache_time)
        time_done = time.time() - time_start_submit
        logger.debug("Done updating mon IV in DB in {} seconds", time_done)

        return encounter_id, is_shiny

    async def _extract_data_or_set_ditto(self, mon_id: int, pokemon_data: pogoprotos.PokemonProto,
                                         pokemon_display: pogoprotos.PokemonDisplayProto):
        if is_mon_ditto_raw(pokemon_data):
            # mon must be a ditto :D
            mon_id = 132
            gender = 3
            move_1 = 242
            move_2 = 133
            form = 0
        else:
            gender = pokemon_display.gender
            move_1 = pokemon_data.move1
            move_2 = pokemon_data.move2
            form = pokemon_display.form
        return form, gender, mon_id, move_1, move_2

    async def mon_lure_iv(self, session: AsyncSession, timestamp: float,
                          encounter_proto: pogoprotos.DiskEncounterOutProto) -> Optional[Tuple[int, datetime]]:
        """
        Update/Insert a lure mon with IVs
        """
        logger.debug3("Updating IV sent for encounter at {}", timestamp)

        pokemon_data: pogoprotos.PokemonProto = encounter_proto.pokemon
        mon_id: int = pokemon_data.pokemon_id
        display: pogoprotos.PokemonDisplayProto = pokemon_data.pokemon_display
        weather_boosted: int = display.weather_boosted_condition
        encounter_id: int = display.display_id

        if encounter_id < 0:
            encounter_id = encounter_id + 2 ** 64

        cache_key = "moniv{}-{}-{}".format(encounter_id, weather_boosted, mon_id)
        if await self._cache.exists(cache_key):
            return None

        # ditto detector
        form, gender, mon_id, move_1, move_2 = await self._extract_data_or_set_ditto(mon_id, pokemon_data,
                                                                                     display)

        capture_probability: pogoprotos.CaptureProbabilityProto = encounter_proto.capture_probability
        capture_probability_list: RepeatedScalarFieldContainer[float] = capture_probability.capture_probability

        now = DatetimeWrapper.fromtimestamp(timestamp)
        time_start_submit = time.time()
        async with session.begin_nested() as nested_transaction:
            mon: Optional[Pokemon] = await PokemonHelper.get(session, encounter_id)
            if not mon:
                mon: Pokemon = Pokemon()
                mon.encounter_id = encounter_id
                mon.latitude = 0
                mon.longitude = 0
                mon.spawnpoint_id = 0
                # TODO: Does this make sense? 2 Minute despawn to at least show it?
                mon.disappear_time = now + timedelta(minutes=2)
                mon.rating_attack = mon.rating_defense = None
            mon.pokemon_id = mon_id
            mon.costume = display.costume
            mon.form = form
            mon.gender = gender
            mon.seen_type = MonSeenTypes.lure_encounter.name
            mon.individual_attack = pokemon_data.individual_attack
            mon.individual_defense = pokemon_data.individual_defense
            mon.individual_stamina = pokemon_data.individual_stamina
            mon.move_1 = move_1
            mon.move_2 = move_2
            mon.cp = pokemon_data.cp
            mon.cp_multiplier = pokemon_data.cp_multiplier
            mon.weight = pokemon_data.weight_kg
            mon.height = pokemon_data.height_m
            if capture_probability_list:
                mon.catch_prob_1 = float(capture_probability_list[0])
                mon.catch_prob_2 = float(capture_probability_list[1])
                mon.catch_prob_3 = float(capture_probability_list[2])
            mon.weather_boosted_condition = weather_boosted
            mon.last_modified = now

            logger.debug("Submitting IV {}", encounter_id)
            session.add(mon)
            await self.maybe_save_ditto(session, display, encounter_id, mon_id, pokemon_data)
            await nested_transaction.commit()
            await self._cache.set(cache_key, 1, ex=REDIS_CACHETIME_MON_LURE_IV)
            time_done = time.time() - time_start_submit
            logger.debug("Done updating mon lure IV in DB in {} seconds", time_done)
        return encounter_id, now

    async def mon_lure_noiv(self, session: AsyncSession, timestamp: float,
                            gmo: pogoprotos.GetMapObjectsOutProto) -> List[int]:
        """
        Update/Insert Lure mons from a map_proto dict
        """
        logger.debug3("DbPogoProtoSubmit::mon_lure_noiv called with data received")
        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = gmo.map_cell
        encounter_ids: List[int] = []
        if not cells:
            return encounter_ids

        for cell in cells:
            for fort in cell.fort:
                lure_mon: pogoprotos.MapPokemonProto = fort.active_pokemon
                mon_id: int = lure_mon.pokedex_type_id
                # TODO: Ensure fort_type is properly checked here
                if fort.fort_type == pogoprotos.FortType.CHECKPOINT and mon_id > 0:
                    encounter_id: int = lure_mon.encounter_id

                    if encounter_id < 0:
                        encounter_id = encounter_id + 2 ** 64
                    encounter_ids.append(encounter_id)
                    cache_key = "monlurenoiv{}".format(encounter_id)
                    if await self._cache.exists(cache_key):
                        continue

                    disappear_time = DatetimeWrapper.fromtimestamp(
                        lure_mon.expiration_time_ms / 1000)

                    now = DatetimeWrapper.fromtimestamp(timestamp)

                    async with session.begin_nested() as nested_transaction:
                        mon: Optional[Pokemon] = await PokemonHelper.get(session, encounter_id)
                        if not mon:
                            display = lure_mon.pokemon_display

                            mon: Pokemon = Pokemon()
                            mon.encounter_id = encounter_id
                            mon.spawnpoint_id = 0
                            mon.seen_type = MonSeenTypes.lure_wild.name
                            mon.pokemon_id = mon_id
                            mon.gender = display.gender
                            mon.costume = display.costume
                            mon.form = display.form
                        mon.weather_boosted_condition = display.weather_boosted_condition
                        mon.latitude = fort.latitude
                        mon.longitude = fort.longitude
                        mon.disappear_time = disappear_time
                        mon.fort_id = fort.fort_id
                        mon.last_modified = now
                        try:
                            logger.debug("Submitting lured non-IV mon {}", encounter_id)
                            session.add(mon)
                            await nested_transaction.commit()
                            await self._cache.set(cache_key, 1, ex=REDIS_CACHETIME_MON_LURE_IV)
                        except sqlalchemy.exc.IntegrityError as e:
                            logger.debug("Failed committing lured non-IV mon {} ({}). Safe to ignore.", encounter_id,
                                         str(e))
                            await nested_transaction.rollback()
        return encounter_ids

    async def update_seen_type_stats(self, session: AsyncSession, **kwargs):
        insert: Dict[int, Dict[MonSeenTypes, datetime]] = {}
        for seen_type in [MonSeenTypes.encounter, MonSeenTypes.wild, MonSeenTypes.nearby_stop,
                          MonSeenTypes.nearby_cell, MonSeenTypes.lure_encounter, MonSeenTypes.lure_wild]:
            encounters = kwargs.get(seen_type.name, None)
            if encounters is None:
                continue

            for encounter_id, seen_time in encounters:
                if encounter_id not in insert:
                    insert[encounter_id] = {}
                insert[encounter_id][seen_type] = seen_time

        for encounter_id, values in insert.items():
            encounter: Optional[datetime] = values.get(MonSeenTypes.encounter, None)
            wild: Optional[datetime] = values.get(MonSeenTypes.wild, None)
            nearby_stop: Optional[datetime] = values.get(MonSeenTypes.nearby_stop, None)
            nearby_cell: Optional[datetime] = values.get(MonSeenTypes.nearby_cell, None)
            lure_encounter: Optional[datetime] = values.get(MonSeenTypes.lure_encounter, None)
            lure_wild: Optional[datetime] = values.get(MonSeenTypes.lure_wild, None)
            async with session.begin_nested() as nested_transaction:
                stat_seen_type: Optional[TrsStatsDetectSeenType] = await TrsStatsDetectSeenTypeHelper.get(session,
                                                                                                          encounter_id)
                if not stat_seen_type:
                    stat_seen_type: TrsStatsDetectSeenType = TrsStatsDetectSeenType()
                    stat_seen_type.encounter_id = encounter_id
                if encounter:
                    stat_seen_type.encounter = encounter
                if wild:
                    stat_seen_type.wild = wild
                if nearby_stop:
                    stat_seen_type.nearby_stop = nearby_stop
                if nearby_cell:
                    stat_seen_type.nearby_cell = nearby_cell
                if lure_encounter:
                    stat_seen_type.lure_encounter = lure_encounter
                if lure_wild:
                    stat_seen_type.lure_wild = lure_wild
                logger.debug("Submitting mon seen stat {}", encounter_id)
                session.add(stat_seen_type)
                try:
                    await nested_transaction.commit()
                except sqlalchemy.exc.IntegrityError as e:
                    await nested_transaction.rollback()
                    logger.debug("Failed submitting stat...")

    async def spawnpoints(self, session: AsyncSession, map_proto: pogoprotos.GetMapObjectsOutProto,
                          received_timestamp: int):
        logger.debug3("DbPogoProtoSubmit::spawnpoints called with data received")
        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = map_proto.map_cell
        if not cells:
            return False
        spawn_ids: List[int] = []
        for cell in cells:
            for wild_mon in cell.wild_pokemon:
                spawn_ids.append(int(str(wild_mon.spawn_point_id), 16))

        spawndef: Dict[int, TrsSpawn] = await self._get_spawndef(session, spawn_ids)
        current_event: Optional[TrsEvent] = await TrsEventHelper.get_current_event(session, True)
        spawns_do_add: List[TrsSpawn] = []
        received_time: datetime = DatetimeWrapper.fromtimestamp(received_timestamp)
        for cell in cells:
            for wild_mon in cell.wild_pokemon:
                spawnid: int = int(str(wild_mon.spawn_point_id), 16)
                lat, lng, _ = S2Helper.get_position_from_cell(
                    int(str(wild_mon.spawn_point_id) + "00000", 16))
                despawntime: int = wild_mon.time_till_hidden_ms

                minpos: Optional[int] = self._get_current_spawndef_pos()
                # TODO: retrieve the spawndefs by a single executemany and pass that...
                spawn: Optional[TrsSpawn] = spawndef.get(spawnid, None)
                if spawn:
                    newspawndef: int = self._set_spawn_see_minutesgroup(spawn.spawndef, minpos)
                else:
                    newspawndef: int = self._set_spawn_see_minutesgroup(self.default_spawndef, minpos)

                # TODO: This may break another known timer...
                if 0 <= int(despawntime) <= 90000:
                    fulldate: datetime = received_time + timedelta(milliseconds=despawntime)
                    earliest_unseen: int = int(despawntime)
                    calcendtime: str = fulldate.strftime("%M:%S")

                    # TODO: First try to fetch a TrsSpawn, then handle the above...
                    # TODO: We can just use the above dict of spawns....
                    if spawn:
                        # Update...
                        # TODO: Is it that simple?
                        spawn.earliest_unseen = min(spawn.earliest_unseen, earliest_unseen)
                        if current_event.id == spawn.eventid or current_event.id != 1 and spawn.eventid != 1:
                            spawn.spawndef = newspawndef
                    else:
                        spawn: TrsSpawn = TrsSpawn()
                        spawn.spawnpoint = spawnid
                        spawn.latitude = lat
                        spawn.longitude = lng
                        spawn.earliest_unseen = earliest_unseen
                        spawn.spawndef = newspawndef
                        spawn.eventid = current_event.id if current_event else 1
                        spawn.first_detection = DatetimeWrapper.fromtimestamp(received_timestamp)
                    spawn.last_scanned = DatetimeWrapper.fromtimestamp(received_timestamp)
                    spawn.calc_endminsec = calcendtime
                else:
                    # TODO: Reduce "complexity..."
                    if spawn:
                        if current_event.id == spawn.eventid or current_event.id != 1 and spawn.eventid != 1:
                            spawn.spawndef = newspawndef
                    else:
                        spawn: TrsSpawn = TrsSpawn()
                        spawn.spawnpoint = spawnid
                        spawn.latitude = lat
                        spawn.longitude = lng
                        spawn.earliest_unseen = 99999999
                        spawn.spawndef = newspawndef
                        spawn.eventid = current_event.id if current_event else 1
                    spawn.last_non_scanned = DatetimeWrapper.now()
                spawns_do_add.append(spawn)
        session.add_all(spawns_do_add)

    async def stops(self, session: AsyncSession, map_proto: pogoprotos.GetMapObjectsOutProto):
        """
        Update/Insert pokestops from a map_proto dict
        """
        logger.debug3("DbPogoProtoSubmit::stops called with data received")
        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = map_proto.map_cell
        if cells is None:
            return False

        for cell in cells:
            cell_id: int = cell.s2_cell_id
            cell_cache_key: str = f"stops_{cell_id}"
            if await self._cache.exists(cell_cache_key):
                continue
            for fort in cell.fort:
                if fort.fort_type == pogoprotos.FortType.CHECKPOINT:
                    await self._handle_pokestop_data(session, fort)
            await self._cache.set(cell_cache_key, 1, ex=REDIS_CACHETIME_CELLS)
        return True

    async def stop_details(self, session: AsyncSession, stop_proto: pogoprotos.FortDetailsOutProto):
        """
        Update/Insert pokestop details from a GMO
        :param session:
        :param stop_proto:
        :return:

        Args:
            session:
            stop_proto:
        """
        logger.debug3("DbPogoProtoSubmit::pokestops_details called")

        stop: Optional[Pokestop] = await self._extract_args_single_stop_details(session, stop_proto)
        if stop:
            # Last modified is not available for stop details!
            # Consider using the FortModifier's expiration timestamps when modifying/adding functionality here
            last_modified: int = 0
            cache_key = "stopdetail{}{}".format(stop.pokestop_id, last_modified)
            if await self._cache.exists(cache_key):
                return True
            async with session.begin_nested() as nested_transaction:
                try:
                    session.add(stop)
                    await nested_transaction.commit()
                    await self._cache.set(cache_key, 1, ex=REDIS_CACHETIME_STOP_DETAILS)
                except sqlalchemy.exc.IntegrityError as e:
                    logger.warning("Failed committing stop details of {} ({})", stop.pokestop_id, str(e))
                    await nested_transaction.rollback()
        return stop is not None

    async def quest(self, session: AsyncSession, quest_proto: pogoprotos.FortSearchOutProto,
                    quest_gen: QuestGen,
                    quest_layer: QuestLayer) -> bool:
        """

        Args:
            quest_layer: the quest layer being scanned
            session:
            quest_proto:
            quest_gen:

        Returns: True if quest was submitted to DB

        """
        logger.debug3("DbPogoProtoSubmit::quest called")
        fort_id: str = quest_proto.fort_id
        if not fort_id:
            return False
        if not quest_proto.challenge_quest:
            return False
        quest: pogoprotos.QuestProto = quest_proto.challenge_quest.quest
        rewards: RepeatedCompositeFieldContainer[pogoprotos.QuestRewardProto] = quest.quest_rewards
        if not rewards:
            return False
        display: pogoprotos.QuestDisplayProto = quest_proto.challenge_quest.quest_display
        quest_title_resource_id = display.title
        reward: pogoprotos.QuestRewardProto = rewards[0]
        item: pogoprotos.ItemRewardProto = reward.item
        encounter: pogoprotos.PokemonEncounterRewardProto = reward.pokemon_encounter
        goal: pogoprotos.QuestGoalProto = quest.goal

        quest_type: int = quest.quest_type.real
        quest_template: Optional[str] = quest.template_id

        reward_type: int = reward.type.real
        item_item: int = item.item.real
        item_amount: int = item.amount
        pokemon_id: Optional[int] = encounter.pokemon_id
        stardust: Optional[int] = reward.stardust

        if reward_type == 4:
            item_amount = reward.candy.amount
            pokemon_id = reward.candy.pokemon_id
        if reward_type == 9:
            item_amount = reward.xl_candy.amount
            pokemon_id = reward.xl_candy.pokemon_id
        elif reward_type == 12:
            item_amount = reward.mega_resource.amount
            pokemon_id = reward.mega_resource.pokemon_id
        elif reward_type == 1:
            # item_amount = reward.get('exp', 0)
            stardust = reward.exp

        # TODO: Check form works like this or .real needed with check for None
        form_id: Optional[int] = encounter.pokemon_display.form
        costume_id: Optional[int] = encounter.pokemon_display.costume
        target: Optional[int] = goal.target
        condition: RepeatedCompositeFieldContainer[pogoprotos.QuestConditionProto] = goal.condition

        # TODO: Json dumping protos...
        json_condition: str = ProtoHelper.to_json(condition)
        task = await quest_gen.questtask(int(quest_type), json_condition, int(target), quest_template,
                                         quest_title_resource_id)
        quest: Optional[TrsQuest] = await TrsQuestHelper.get(session, fort_id, quest_layer)
        if not quest:
            quest = TrsQuest()
            quest.GUID = fort_id
            quest.layer = quest_layer.value
        quest.quest_type = quest_type
        quest.quest_timestamp = int(time.time())
        quest.quest_stardust = stardust
        quest.quest_pokemon_id = pokemon_id
        quest.quest_pokemon_form_id = form_id
        quest.quest_pokemon_costume_id = costume_id
        quest.quest_reward_type = reward_type
        quest.quest_item_id = item_item
        quest.quest_item_amount = item_amount
        quest.quest_target = target
        quest.quest_condition = json_condition
        quest.quest_reward = ProtoHelper.to_json(rewards)
        quest.quest_task = task
        quest.quest_template = quest_template
        quest.quest_title = quest_title_resource_id

        logger.debug3("DbPogoProtoSubmit::quest submitted quest type {} at stop {}", quest_type, fort_id)
        async with session.begin_nested() as nested_transaction:
            try:
                session.add(quest)
                await nested_transaction.commit()
            except sqlalchemy.exc.IntegrityError as e:
                logger.warning("Failed committing quest of stop {}, ({})", fort_id, str(e))
                await nested_transaction.rollback()
        return True

    async def gyms(self, session: AsyncSession, map_proto: pogoprotos.GetMapObjectsOutProto, received_timestamp: int):
        """
        Update/Insert gyms from a map_proto dict
        """
        logger.debug3("DbPogoProtoSubmit::gyms called with data received from")
        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = map_proto.map_cell
        if not cells:
            return False
        time_receiver: datetime = DatetimeWrapper.fromtimestamp(received_timestamp)
        for cell in cells:
            cell_id: int = cell.s2_cell_id
            cell_cache_key: str = f"gyms_{cell_id}"
            if await self._cache.exists(cell_cache_key):
                continue
            for gym in cell.fort:
                if gym.fort_type == pogoprotos.FortType.GYM:
                    gymid: str = gym.fort_id
                    last_modified_ts: float = gym.last_modified_ms / 1000
                    last_modified: datetime = DatetimeWrapper.fromtimestamp(
                        last_modified_ts)
                    s2_cell_id = S2Helper.lat_lng_to_cell_id(gym.latitude, gym.longitude)
                    weather: Optional[Weather] = await WeatherHelper.get(session, str(s2_cell_id))
                    gameplay_weather: int = weather.gameplay_weather if weather is not None else 0
                    cache_key = "gym{}{}{}".format(gymid, last_modified_ts, gameplay_weather)
                    if await self._cache.exists(cache_key):
                        continue
                    guard_pokemon_id: int = gym.guard_pokemon_id
                    team_id: int = gym.team

                    gym_obj: Optional[Gym] = await GymHelper.get(session, gymid)
                    if not gym_obj:
                        gym_obj: Gym = Gym()
                        gym_obj.gym_id = gymid
                    gym_obj.team_id = team_id
                    gym_obj.guard_pokemon_id = guard_pokemon_id
                    gym_obj.slots_available = gym.gym_display.slots_available
                    gym_obj.enabled = gym.enabled
                    gym_obj.latitude = gym.latitude
                    gym_obj.longitude = gym.longitude
                    gym_obj.total_cp = gym.gym_display.total_gym_cp
                    gym_obj.is_in_battle = gym.is_in_battle
                    gym_obj.last_modified = last_modified
                    gym_obj.last_scanned = time_receiver
                    gym_obj.is_ex_raid_eligible = gym.is_ex_raid_eligible
                    gym_obj.is_ar_scan_eligible = gym.is_ar_scan_eligible
                    gym_obj.weather_boosted_condition = gameplay_weather

                    gym_detail: Optional[GymDetail] = await GymDetailHelper.get(session, gymid)
                    if not gym_detail:
                        gym_detail: GymDetail = GymDetail()
                        gym_detail.gym_id = gymid
                        gym_detail.name = "unknown"
                        gym_detail.url = ""
                    gym_url: Optional[str] = gym.image_url
                    if gym_url and gym_url.strip():
                        gym_detail.url = gym_url.strip()
                    gym_detail.last_scanned = time_receiver
                    async with session.begin_nested() as nested_transaction:
                        try:
                            session.add(gym_obj)
                            session.add(gym_detail)
                            await nested_transaction.commit()
                            await self._cache.set(cache_key, 1, ex=REDIS_CACHETIME_GYMS)
                        except sqlalchemy.exc.IntegrityError as e:
                            logger.warning("Failed committing gym data of {} ({})", gymid, str(e))
                            await nested_transaction.rollback()
            # done processing cell
            await self._cache.set(cell_cache_key, 1, ex=REDIS_CACHETIME_CELLS)
        return True

    async def gym_info(self, session: AsyncSession, gym_info: pogoprotos.GymGetInfoOutProto):
        """
        Update gyms from a map_proto dict
        """
        logger.debug3("Updating gyms")
        if gym_info.result != 1:
            return False
        status: Optional[pogoprotos.GymStatusAndDefendersProto] = gym_info.gym_status_and_defenders
        if status is None:
            return False
        fort_proto: Optional[pogoprotos.PokemonFortProto] = status.pokemon_fort_proto
        if fort_proto is None:
            return False
        gym_id: str = fort_proto.fort_id

        gym_detail: Optional[GymDetail] = await GymDetailHelper.get(session, gym_id)
        if not gym_detail:
            return False
        touched: bool = False
        if gym_info.name:
            touched = True
            gym_detail.name = gym_info.name
        if gym_info.description:
            touched = True
            gym_detail.description = gym_info.description
        if gym_info.url:
            touched = True
            gym_detail.url = gym_info.url
        if touched:
            async with session.begin_nested() as nested_transaction:
                try:
                    session.add(gym_detail)
                    await nested_transaction.commit()
                except sqlalchemy.exc.IntegrityError as e:
                    logger.warning("Failed committing gym info {} ({})", gym_id, str(e))
                    await nested_transaction.rollback()
        return True

    async def raids(self, session: AsyncSession, map_proto: pogoprotos.GetMapObjectsOutProto, timestamp: int) -> int:
        """
        Update/Insert raids from a map_proto dict

        Returns: amount of raids in GMO processed
        """
        logger.debug3("DbPogoProtoSubmit::raids called with data received")
        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = map_proto.map_cell
        if not cells:
            return False
        raids_seen: int = 0
        received_at: datetime = DatetimeWrapper.fromtimestamp(timestamp)
        for cell in cells:
            for gym in cell.fort:
                if gym.fort_type == pogoprotos.FortType.GYM and gym.raid_info.raid_end_ms > 0:
                    if gym.raid_info.raid_pokemon:
                        raids_seen += 1
                        raid_info: pogoprotos.RaidInfoProto = gym.raid_info

                        pokemon_id: Optional[int] = raid_info.raid_pokemon.pokemon_id.real
                        cp: int = raid_info.raid_pokemon.cp
                        move_1: int = raid_info.raid_pokemon.move1
                        move_2: int = raid_info.raid_pokemon.move2
                        form: Optional[int] = raid_info.raid_pokemon.pokemon_display.form
                        gender: Optional[int] = raid_info.raid_pokemon.pokemon_display.gender
                        costume: Optional[int] = raid_info.raid_pokemon.pokemon_display.costume
                        evolution: Optional[int] = raid_info.raid_pokemon.pokemon_display.current_temp_evolution
                    else:
                        pokemon_id: Optional[int] = None
                        cp: int = 0
                        move_1 = 1
                        move_2 = 2
                        form: Optional[int] = None
                        gender: Optional[int] = None
                        costume: Optional[int] = None
                        evolution: Optional[int] = 0

                    raid_end_sec: int = int(gym.raid_info.raid_end_ms / 1000)
                    raidend_date = DatetimeWrapper.fromtimestamp(
                        float(raid_end_sec))

                    gymid: str = gym.fort_id

                    logger.debug3("Adding/Updating gym {} with level {} ending at {}", gymid, gym.raid_info.raid_level,
                                  raidend_date.strftime("%Y-%m-%d %H:%M:%S"))

                    cache_key = "raid{}{}{}".format(gymid, pokemon_id, raid_end_sec)
                    if await self._cache.exists(cache_key):
                        continue

                    raid_spawn_sec: int = int(gym.raid_info.raid_spawn_ms / 1000)
                    raid_battle_sec: int = int(gym.raid_info.raid_battle_ms / 1000)
                    raidspawn_date = DatetimeWrapper.fromtimestamp(float(raid_spawn_sec))
                    raidstart_date = DatetimeWrapper.fromtimestamp(float(raid_battle_sec))

                    raid: Optional[Raid] = await RaidHelper.get(session, gymid)
                    if not raid:
                        raid: Raid = Raid()
                        raid.gym_id = gymid
                    elif raid.last_scanned > received_at:
                        continue
                    raid.level = gym.raid_info.raid_level
                    raid.spawn = raidspawn_date
                    raid.start = raidstart_date
                    raid.end = raidend_date
                    raid.pokemon_id = pokemon_id
                    raid.cp = cp
                    raid.move_1 = move_1
                    raid.move_2 = move_2
                    raid.last_scanned = received_at
                    raid.form = form
                    raid.is_exclusive = 0
                    raid.gender = gender
                    raid.costume = costume
                    raid.evolution = evolution
                    async with session.begin_nested() as nested_transaction:
                        try:
                            session.add(raid)
                            await nested_transaction.commit()
                            await self._cache.set(cache_key, 1, ex=REDIS_CACHETIME_RAIDS)
                        except sqlalchemy.exc.IntegrityError as e:
                            logger.warning("Failed committing raid for gym {} ({})", gymid, str(e))
                            await nested_transaction.rollback()
        logger.debug3("DbPogoProtoSubmit::raids: Done submitting raids with data received")
        return raids_seen

    async def routes(self, session: AsyncSession, routes_proto: pogoprotos.GetRoutesOutProto,
                     timestamp_received: int) -> None:
        logger.debug3("DbPogoProtoSubmit::routes called with data received")
        status: int = routes_proto.status
        # The status of the GetRoutesOutProto indicates the reset of the values being useful for us where a value of 1
        #  maps to success
        if status != 1:
            logger.warning("Routes response not useful ({})", status)
            return

        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientRouteMapCellProto] = routes_proto.route_map_cell
        if not cells:
            logger.warning("No cells to process in routes proto")
            return

        for cell in cells:
            s2_cell_id: int = cell.s2_cell_id
            routes: RepeatedCompositeFieldContainer[pogoprotos.SharedRouteProto] = cell.route
            if not routes:
                continue
            for route in routes:
                await self._handle_route_cell(session, s2_cell_id, route, timestamp_received)

    async def _handle_route_cell(self, session: AsyncSession, s2_cell_id: int, route_data: pogoprotos.SharedRouteProto,
                                 timestamp_received: int) -> None:
        route_id: str = route_data.id
        cache_key = "route{}".format(route_id)
        if await self._cache.exists(cache_key):
            return
        date_received: datetime = DatetimeWrapper.fromtimestamp(timestamp_received)
        async with session.begin_nested() as nested_transaction:
            try:
                route: Optional[Route] = await RouteHelper.get(session, route_id)
                if not route:
                    route: Route = Route()
                    route.route_id = route_id

                route.waypoints = ProtoHelper.to_json(route_data.waypoints)
                route.type = route_data.type
                route.path_type = route_data.path_type
                route.name = route_data.name
                route.version = route_data.version
                route.description = route_data.description
                route.reversible = route_data.reversible

                submission_time_raw: int = route_data.submission_time
                logger.debug2("Submission time raw: {}", submission_time_raw)
                submission_time: datetime = DatetimeWrapper.fromtimestamp(submission_time_raw / 1000)
                route.submission_time = submission_time
                route.route_distance_meters = route_data.route_distance_meters
                route.route_duration_seconds = route_data.route_duration_seconds

                pins_raw: RepeatedCompositeFieldContainer[pogoprotos.RoutePin] = route_data.pins
                route.pins = ProtoHelper.to_json(pins_raw)

                tags_raw: RepeatedScalarFieldContainer[str] = route_data.tags
                route.tags = ProtoHelper.to_json(tags_raw)

                image_data: pogoprotos.RouteImageProto = route_data.image
                route.image = image_data.image_url
                route.image_border_color_hex = image_data.border_color_hex

                route_submission_status_data: pogoprotos.RouteSubmissionStatus = route_data.route_submission_status[-1]
                route.route_submission_status = route_submission_status_data.status
                route_submission_update_time: int = route_submission_status_data.submission_status_update_time_ms
                route.route_submission_update_time = DatetimeWrapper.fromtimestamp(route_submission_update_time / 1000)

                start_poi_data: pogoprotos.RoutePoiAnchor = route_data.start_poi
                start_poi_anchor: pogoprotos.RouteWaypointProto = start_poi_data.anchor
                route.start_poi_fort_id = start_poi_anchor.fort_id
                route.start_poi_latitude = start_poi_anchor.lat_degrees
                route.start_poi_longitude = start_poi_anchor.lng_degrees
                route.start_poi_image_url = start_poi_data.image_url

                end_poi_data: pogoprotos.RoutePoiAnchor = route_data.end_poi
                end_poi_anchor: pogoprotos.RouteWaypointProto = end_poi_data.anchor
                route.end_poi_fort_id = end_poi_anchor.fort_id
                route.end_poi_latitude = end_poi_anchor.lat_degrees
                route.end_poi_longitude = end_poi_anchor.lng_degrees
                route.end_poi_image_url = end_poi_data.image_url

                route.last_updated = date_received

                session.add(route)
                await self._cache.set(cache_key, 1, ex=REDIS_CACHETIME_ROUTE)
                await nested_transaction.commit()
            except sqlalchemy.exc.IntegrityError as e:
                logger.warning("Failed committing route {} of cell {} ({})", route_id, s2_cell_id, str(e))
                await nested_transaction.rollback()

    async def weather(self, session: AsyncSession, map_proto: pogoprotos.GetMapObjectsOutProto,
                      received_timestamp: int) -> bool:
        """
        Update/Insert weather from a map_proto dict
        """
        logger.debug3("DbPogoProtoSubmit::weather called with data received")
        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = map_proto.map_cell
        if not cells:
            return False

        for client_weather in map_proto.client_weather:
            time_of_day: int = map_proto.time_of_day
            await self._handle_weather_data(session, client_weather, time_of_day, received_timestamp)
        return True

    async def cells(self, session: AsyncSession, map_proto: pogoprotos.GetMapObjectsOutProto):
        protocells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = map_proto.map_cell

        for cell in protocells:
            cell_id: int = cell.s2_cell_id

            if cell_id < 0:
                cell_id = cell_id + 2 ** 64
            cell_cache_key = "s2cell{}".format(cell_id)
            if await self._cache.exists(cell_cache_key):
                continue
            await self._cache.set(cell_cache_key, 1, ex=REDIS_CACHETIME_CELLS)
            logger.debug3("Updating s2cell {}", cell_id)
            try:
                await TrsS2CellHelper.insert_update_cell(session, cell)
            except sqlalchemy.exc.IntegrityError as e:
                logger.debug("Failed committing cell {} ({})", cell_id, str(e))
                await self._cache.set(cell_cache_key, 1, ex=1)

    async def _handle_single_incident(self, session: AsyncSession,
                                      stop_id: str,
                                      incident_data: Optional[pogoprotos.PokestopIncidentDisplayProto]):
        if not incident_data:
            logger.warning("Incident data is empty")
            return
        incident_id: Optional[str] = incident_data.incident_id
        if incident_id is None or len(incident_id.strip()) == 0:
            return
        logger.debug2("Handling incident '{}': {}", incident_id, incident_data)
        incident: Optional[PokestopIncident] = await PokestopIncidentHelper.get(session,
                                                                                stop_id,
                                                                                incident_id)
        if not incident:
            incident = PokestopIncident()
            incident.pokestop_id = stop_id
            incident.incident_id = incident_id
        incident_start: float = incident_data.incident_start_ms / 1000
        if incident_start > 0:
            incident.incident_start = DatetimeWrapper.fromtimestamp(incident_start)

        incident_expiration: float = incident_data.incident_expiration_ms / 1000
        if incident_expiration > 0:
            incident.incident_expiration = DatetimeWrapper.fromtimestamp(incident_expiration)

        incident.hide_incident = incident_data.hide_incident
        incident.incident_display_type = incident_data.incident_display_type
        incident.incident_display_order_priority = incident_data.incident_display_order_priority
        incident.custom_display = incident_data.custom_display.style_config_address
        incident.is_cross_stop_incident = incident_data.is_cross_stop_incident

        character_display: Optional[pogoprotos.CharacterDisplayProto] = incident_data.character_display
        incident.character_display = character_display.character if character_display else 0

        async with session.begin_nested() as nested_transaction:
            try:
                logger.debug("Adding or updating incident {}", incident_id)
                session.add(incident)
                await nested_transaction.commit()
            except sqlalchemy.exc.IntegrityError as e:
                logger.warning("Failed committing incident {} for pokestop {} ({})",
                               incident_id, stop_id, str(e))
                await nested_transaction.rollback()

    async def _handle_pokestop_incident_data(self, session: AsyncSession,
                                             stop_id: str,
                                             stop_data: pogoprotos.PokemonFortProto):
        if stop_data.pokestop_display:
            await self._handle_single_incident(session, stop_id, stop_data.pokestop_display)
        incident_displays: Optional[RepeatedCompositeFieldContainer[pogoprotos.PokestopIncidentDisplayProto]] = (
            stop_data.pokestop_displays)
        if incident_displays:
            for incident in incident_displays:
                await self._handle_single_incident(session, stop_id, incident)

    async def _handle_pokestop_data(self, session: AsyncSession,
                                    stop_data: pogoprotos.PokemonFortProto) -> Optional[Pokestop]:
        if stop_data.fort_type != pogoprotos.FortType.CHECKPOINT:
            logger.info("{} is not a pokestop", stop_data)
            return

        # We can detect changes of the incidents by simply appending all incident IDs sent in the proto I guess...
        stop_id: str = stop_data.fort_id
        last_modified_timestamp: int = stop_data.last_modified_ms
        if not last_modified_timestamp:
            last_modified_timestamp = int(math.ceil(DatetimeWrapper.now().timestamp() / 1000)) * 1000
        cache_key = "stop{}{}".format(stop_id, last_modified_timestamp)
        if await self._cache.exists(cache_key):
            return

        now = DatetimeWrapper.fromtimestamp(time.time())
        last_modified: datetime = DatetimeWrapper.fromtimestamp(
            stop_data.last_modified_ms / 1000
        )
        lure: datetime = DatetimeWrapper.fromtimestamp(0)
        active_fort_modifier: Optional[int] = None
        is_ar_scan_eligible = stop_data.is_ar_scan_eligible

        if len(stop_data.active_fort_modifier) > 0:
            # get current lure duration
            trs_event: Optional[TrsEvent] = await TrsEventHelper.get_current_event(session)
            if trs_event and trs_event.event_lure_duration:
                lure_duration = int(trs_event.event_lure_duration)
            else:
                lure_duration = int(30)

            active_fort_modifier = stop_data.active_fort_modifier[0]
            lure = DatetimeWrapper.fromtimestamp(
                lure_duration * 60 + (stop_data.last_modified_ms / 1000)
            )

        pokestop: Optional[Pokestop] = await PokestopHelper.get(session, stop_id)
        if not pokestop:
            pokestop: Pokestop = Pokestop()
            pokestop.pokestop_id = stop_id
        pokestop.enabled = stop_data.enabled
        pokestop.latitude = stop_data.latitude
        pokestop.longitude = stop_data.longitude
        pokestop.last_modified = last_modified
        pokestop.lure_expiration = lure
        pokestop.last_updated = now
        pokestop.active_fort_modifier = active_fort_modifier
        pokestop.is_ar_scan_eligible = is_ar_scan_eligible
        async with session.begin_nested() as nested_transaction:
            try:
                session.add(pokestop)
                await nested_transaction.commit()
                await self._cache.set(cache_key, 1, ex=REDIS_CACHETIME_POKESTOP_DATA)
            except sqlalchemy.exc.IntegrityError as e:
                logger.warning("Failed committing stop {} ({})", stop_id, str(e))
                await session.rollback()
        await self._handle_pokestop_incident_data(session, stop_id, stop_data)

    async def _extract_args_single_stop_details(self, session: AsyncSession,
                                                stop_data: pogoprotos.FortDetailsOutProto) -> Optional[Pokestop]:
        if stop_data.fort_type != pogoprotos.FortType.CHECKPOINT:
            return None
        image: RepeatedScalarFieldContainer[str] = stop_data.image_url
        now: datetime = DatetimeWrapper.now()
        pokestop: Optional[Pokestop] = await PokestopHelper.get(session, stop_data.id)
        if not pokestop:
            pokestop: Pokestop = Pokestop()
            pokestop.pokestop_id = stop_data.id
            pokestop.enabled = True
            pokestop.last_modified = DatetimeWrapper.fromtimestamp(0)
        elif pokestop.last_updated > now:
            return None
        pokestop.latitude = stop_data.latitude
        pokestop.longitude = stop_data.longitude
        pokestop.name = stop_data.name
        if image and image[0]:
            pokestop.image = image[0]
        pokestop.last_updated = now
        return pokestop

    async def _handle_weather_data(self, session: AsyncSession, client_weather_data: pogoprotos.ClientWeatherProto,
                                   time_of_day: int,
                                   received_timestamp: int) -> None:
        cell_id: int = client_weather_data.s2_cell_id
        real_lat, real_lng = S2Helper.middle_of_cell(cell_id)
        display_weather_data: Optional[pogoprotos.DisplayWeatherProto] = client_weather_data.display_weather
        if not display_weather_data:
            return
        else:
            gameplay_weather: int = client_weather_data.gameplay_weather.gameplay_condition
        cache_key = "weather{}{}{}{}{}{}{}".format(cell_id, display_weather_data.rain_level,
                                                   display_weather_data.wind_level,
                                                   display_weather_data.snow_level,
                                                   display_weather_data.fog_level,
                                                   display_weather_data.wind_direction,
                                                   gameplay_weather)
        if await self._cache.exists(cache_key):
            return
        date_received = DatetimeWrapper.fromtimestamp(received_timestamp)
        async with session.begin_nested() as nested_transaction:
            try:
                weather: Optional[Weather] = await WeatherHelper.get(session, str(cell_id))
                if not weather:
                    weather: Weather = Weather()
                    weather.s2_cell_id = str(cell_id)
                    weather.latitude = real_lat
                    weather.longitude = real_lng
                weather.cloud_level = display_weather_data.cloud_level
                weather.rain_level = display_weather_data.rain_level
                weather.wind_level = display_weather_data.wind_level
                weather.snow_level = display_weather_data.snow_level
                weather.fog_level = display_weather_data.fog_level
                weather.wind_direction = display_weather_data.wind_direction
                weather.gameplay_weather = gameplay_weather
                alerts: RepeatedCompositeFieldContainer[pogoprotos.WeatherAlertProto] = client_weather_data.alerts
                weather.warn_weather = alerts[0].warn_weather if alerts else 0
                weather.severity = alerts[0].severity if alerts else 0
                weather.world_time = time_of_day
                weather.last_updated = date_received

                if not weather:
                    return

                session.add(weather)
                await self._cache.set(cache_key, 1, ex=REDIS_CACHETIME_WEATHER)
                await nested_transaction.commit()
            except sqlalchemy.exc.IntegrityError as e:
                logger.warning("Failed committing weather of cell {} ({})", cell_id, str(e))
                await nested_transaction.rollback()

    async def _get_spawndef(self, session: AsyncSession, spawn_ids: List[int]) -> Dict[int, TrsSpawn]:
        if not spawn_ids:
            return {}
        logger.debug3("DbPogoProtoSubmit::_get_spawndef called")

        spawnret = {}
        res: List[TrsSpawn] = await TrsSpawnHelper.get_all(session, spawn_ids)
        for spawn in res:
            spawnret[int(spawn.spawnpoint)] = spawn
        return spawnret

    def _get_current_spawndef_pos(self) -> Optional[int]:
        minute_value = int(DatetimeWrapper.now().strftime("%M"))
        if minute_value < 15:
            pos = 4
        elif minute_value < 30:
            pos = 5
        elif minute_value < 45:
            pos = 6
        elif minute_value < 60:
            pos = 7
        else:
            pos = None
        return pos

    def _set_spawn_see_minutesgroup(self, spawndef, pos):
        minte_group = BitArray(uint=spawndef, length=8)
        if pos == 4:
            minte_group[0] = 0
            minte_group[4] = 1
        if pos == 5:
            minte_group[1] = 0
            minte_group[5] = 1
        if pos == 6:
            minte_group[2] = 0
            minte_group[6] = 1
        if pos == 7:
            minte_group[3] = 0
            minte_group[7] = 1
        return minte_group.uint

    def get_time_ms(self):
        return int(time.time() * 1000)

    async def maybe_save_ditto(self, session: AsyncSession, display: pogoprotos.PokemonDisplayProto,
                               encounter_id: int, mon_id: int,
                               pokemon_data: pogoprotos.PokemonProto):
        if mon_id == 132:
            # Save ditto disguise
            await PokemonDisplayHelper.insert_ignore(session, encounter_id,
                                                     pokemon_id=pokemon_data.pokemon_id,
                                                     form=display.form,
                                                     gender=display.gender,
                                                     costume=display.costume)

    async def stations(self, session: AsyncSession, received_timestamp: int,
                       map_proto: pogoprotos.GetMapObjectsOutProto) -> int:
        logger.debug3("DbPogoProtoSubmit::stations called with data received")
        cells: RepeatedCompositeFieldContainer[pogoprotos.ClientMapCellProto] = map_proto.map_cell
        if not cells:
            return False

        received_at: datetime = DatetimeWrapper.fromtimestamp(received_timestamp)
        stations_seen: int = 0

        for cell in cells:
            station: pogoprotos.StationProto
            for station in cell.stations:
                station_id: str = station.id
                start_time_ms: float = station.start_time_ms
                battle_spawn_ms: float = station.battle_details.battle_spawn_ms

                station_cache_key = "station_{}_{}_{}".format(station_id, start_time_ms, battle_spawn_ms)
                if await self._cache.exists(station_cache_key):
                    continue

                latitude: float = station.lat
                longitude: float = station.lng
                name: str = station.name
                start_time = DatetimeWrapper.fromtimestamp(float(start_time_ms / 1000))
                end_time = DatetimeWrapper.fromtimestamp(float(station.end_time_ms / 1000))
                bread_battle_available: bool = station.is_bread_battle_available
                inactive: bool = station.is_inactive

                stations_seen += 1
                logger.debug3(
                    "Station detected, id: {}, name: {}, lat: {}, lng: {}, start: {}, end: {}, available: {}, "
                    "inactive: {}",
                    station_id, name, latitude, longitude, start_time, end_time, bread_battle_available, inactive)
                station_obj: Optional[Station] = await StationHelper.get(session, station_id)
                if not station_obj:
                    station_obj: Station = Station()
                    station_obj.station_id = station_id
                    station_obj.latitude = latitude
                    station_obj.longitude = longitude
                    station_obj.name = name

                if station.battle_details and station.battle_details.battle_window_end_ms > 0:
                    bdetails: pogoprotos.BreadBattleDetailProto = station.battle_details
                    station_obj.battle_spawn = DatetimeWrapper.fromtimestamp(float(bdetails.battle_spawn_ms / 1000))
                    station_obj.battle_window_start = DatetimeWrapper.fromtimestamp(
                        float(bdetails.battle_window_start_ms / 1000))
                    station_obj.battle_window_end = DatetimeWrapper.fromtimestamp(
                        float(bdetails.battle_window_end_ms / 1000))
                    station_obj.battle_level = bdetails.battle_level

                    if bdetails.reward_pokemon:
                        pokemon_data: pogoprotos.PokemonProto = bdetails.reward_pokemon
                        if pokemon_data.pokemon_id and pokemon_data.pokemon_id > 0:
                            station_obj.reward_pokemon_id = pokemon_data.pokemon_id
                            station_obj.reward_pokemon_form = pokemon_data.pokemon_display.form
                            station_obj.reward_pokemon_gender = pokemon_data.pokemon_display.gender
                            station_obj.reward_pokemon_costume = pokemon_data.pokemon_display.costume
                            station_obj.reward_pokemon_alignment = pokemon_data.pokemon_display.alignment
                            station_obj.reward_pokemon_bread_mode = pokemon_data.pokemon_display.bread_mode_enum

                    if bdetails.battle_pokemon:
                        pokemon_data: pogoprotos.PokemonProto = bdetails.battle_pokemon
                        if pokemon_data.pokemon_id and pokemon_data.pokemon_id > 0:
                            station_obj.battle_pokemon_id = pokemon_data.pokemon_id
                            station_obj.battle_pokemon_form = pokemon_data.pokemon_display.form
                            station_obj.battle_pokemon_gender = pokemon_data.pokemon_display.gender
                            station_obj.battle_pokemon_costume = pokemon_data.pokemon_display.costume
                            station_obj.battle_pokemon_alignment = pokemon_data.pokemon_display.alignment
                            station_obj.battle_pokemon_bread_mode = pokemon_data.pokemon_display.bread_mode_enum
                            station_obj.battle_pokemon_move_1 = pokemon_data.move1
                            station_obj.battle_pokemon_move_2 = pokemon_data.move2

                station_obj.start_time = start_time
                station_obj.end_time = end_time
                station_obj.bread_battle_available = bread_battle_available
                station_obj.inactive = inactive
                station_obj.last_updated = received_at
                async with session.begin_nested() as nested_transaction:
                    try:
                        session.add(station_obj)
                        await nested_transaction.commit()
                        await self._cache.set(station_cache_key, 1, ex=REDIS_CACHETIME_STATIONS)
                    except sqlalchemy.exc.IntegrityError as e:
                        logger.error("Failed committing station data of {} ({})", station_id, str(e))
                        logger.error(e)
                        await nested_transaction.rollback()
                        await self._cache.set(station_cache_key, 1, ex=1)
        logger.debug3("DbPogoProtoSubmit::stations: Done submitting stations with data received")
        return stations_seen
