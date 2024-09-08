import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import sqlalchemy
from google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from loguru import logger
from redis.asyncio import Redis
from sqlalchemy.exc import IntegrityError

from mapadroid.account_handler.AbstractAccountHandler import \
    AbstractAccountHandler
from mapadroid.data_handler.mitm_data.AbstractMitmMapper import \
    AbstractMitmMapper
from mapadroid.data_handler.stats.AbstractStatsHandler import \
    AbstractStatsHandler
from mapadroid.db.DbPogoProtoSubmitRaw import DbPogoProtoSubmitRaw
from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.db.helper.SettingsDeviceHelper import SettingsDeviceHelper
from mapadroid.db.model import SettingsDevice
from mapadroid.mitm_receiver.protos.ProtoHelper import ProtoHelper
from mapadroid.utils.DatetimeWrapper import DatetimeWrapper
from mapadroid.utils.ProtoIdentifier import ProtoIdentifier
from mapadroid.utils.gamemechanicutil import determine_current_quest_layer
from mapadroid.utils.madGlobals import (MadGlobals, MitmReceiverRetry,
                                        MonSeenTypes, QuestLayer)
from mapadroid.utils.questGen import QuestGen
import mapadroid.mitm_receiver.protos.Rpc_pb2 as pogoprotos


class SerializedMitmDataProcessor:
    def __init__(self, data_queue: asyncio.Queue, stats_handler: AbstractStatsHandler,
                 mitm_mapper: AbstractMitmMapper, db_wrapper: DbWrapper, quest_gen: QuestGen,
                 account_handler: AbstractAccountHandler,
                 name=None):
        self.__queue: asyncio.Queue = data_queue
        self.__db_wrapper: DbWrapper = db_wrapper
        self.__db_submit: DbPogoProtoSubmitRaw = db_wrapper.proto_submit
        self.__stats_handler: AbstractStatsHandler = stats_handler
        self.__mitm_mapper: AbstractMitmMapper = mitm_mapper
        self.__quest_gen: QuestGen = quest_gen
        self.__name = name
        self.__account_handler: AbstractAccountHandler = account_handler

    async def run(self):
        logger.info("Starting serialized MITM data processor")
        # TODO: use event to stop... Remove try/catch...
        with logger.contextualize(identifier=self.__name, name="mitm-processor"):
            while True:
                try:
                    item = await self.__queue.get()
                    if item is None:
                        logger.info("Received signal to stop MITM data processor")
                        break
                    threshold_seconds = MadGlobals.application_args.mitm_ignore_proc_time_thresh
                    start_time = self.get_time_ms()
                    if threshold_seconds > 0:
                        minimum_timestamp = (start_time / 1000) - threshold_seconds
                        if item[0] < minimum_timestamp:
                            logger.debug(
                                "Data received at {} is older than configured threshold of {}s ({}). Ignoring data.",
                                item[0], threshold_seconds,
                                DatetimeWrapper.fromtimestamp(minimum_timestamp))
                            return
                    try:
                        with logger.contextualize(identifier=item[2], name="mitm-processor"):
                            if item[1].get("raw", False):
                                await self._process_data_raw(received_timestamp=item[0], data=item[1],
                                                             origin=item[2])
                            else:
                                logger.error("Only raw processing is supported now.")
                        del item
                    except (sqlalchemy.exc.IntegrityError, MitmReceiverRetry, sqlalchemy.exc.InternalError) as e:
                        logger.info("Failed submitting data to DB, rescheduling. {}", e)
                        await self.__queue.put(item)
                    except Exception as e:
                        logger.exception(e)
                        logger.info("Failed processing data. {}", e)
                    self.__queue.task_done()
                    end_time = self.get_time_ms() - start_time
                    logger.debug("MITM data processor {} finished queue item in {}ms", self.__name, end_time)
                except KeyboardInterrupt:
                    logger.info("Received keyboard interrupt, stopping MITM data processor")
                    return

    async def _process_data_raw(self, received_timestamp: int, data: Dict, origin: str):
        method_id: Optional[int] = data.get("type", None)
        logger.debug("Processing received data")
        processed_timestamp: datetime = DatetimeWrapper.fromtimestamp(received_timestamp)
        if not method_id or not data.get("raw", False):
            logger.error("Data received from {} does not contain a valid method ID or is not in raw format")
            return
        start_time = self.get_time_ms()
        if method_id == ProtoIdentifier.GMO.value:
            await self.__process_gmo_raw(data, origin, processed_timestamp, received_timestamp, start_time)
        elif method_id == ProtoIdentifier.ENCOUNTER.value:
            await self.__process_encounter(data, origin, processed_timestamp, received_timestamp, start_time)
        elif method_id == ProtoIdentifier.DISK_ENCOUNTER.value:
            # lure mons with iv
            await self.__process_lured_encounter(data, origin, processed_timestamp, received_timestamp, start_time)
        elif method_id == ProtoIdentifier.FORT_SEARCH.value:
            logger.debug("Processing proto 101 (FORT_SEARCH)")
            async with self.__db_wrapper as session, session:
                try:
                    fort_search: pogoprotos.FortSearchOutProto = pogoprotos.FortSearchOutProto()
                    fort_search.ParseFromString(data["payload"])
                    # TODO: Check enum works with int comparison
                    if fort_search.result == 1:
                        async with session.begin_nested() as nested_transaction:
                            try:
                                await nested_transaction.commit()
                            except sqlalchemy.exc.IntegrityError as e:
                                logger.warning("Failed marking stop {} as visited ({})", fort_search.fort_id, str(e))
                                await nested_transaction.rollback()
                        quest_layer: QuestLayer = determine_current_quest_layer(await self.__mitm_mapper
                                                                                .get_quests_held(origin))
                        new_quest: bool = await self.__db_submit.quest(session, fort_search, self.__quest_gen,
                                                                       quest_layer)
                        if new_quest:
                            await self.__stats_handler.stats_collect_quest(origin, processed_timestamp)
                        await session.commit()
                except Exception as e:
                    logger.warning("Failed submitting quests to DB: {}", e)

            end_time = self.get_time_ms() - start_time
            logger.debug("Done processing proto 101 in {}ms", end_time)
        elif method_id == ProtoIdentifier.FORT_DETAILS.value:
            logger.debug("Processing proto 104 (FORT_DETAILS)")
            async with self.__db_wrapper as session, session:
                try:
                    fort_details: pogoprotos.FortDetailsOutProto = pogoprotos.FortDetailsOutProto()
                    fort_details.ParseFromString(data["payload"])
                    await self.__db_submit.stop_details(session, fort_details)
                    await session.commit()
                except Exception as e:
                    logger.warning("Failed fort details to DB: {}", e)

            end_time = self.get_time_ms() - start_time
            logger.debug("Done processing proto 104 in {}ms", end_time)
        elif method_id == ProtoIdentifier.INVENTORY.value:
            logger.debug("Processing proto 4 (GET_HOLO_INVENTORY)")
            await self._handle_inventory_data(origin, data["payload"])
            end_time = self.get_time_ms() - start_time
            logger.debug("Done processing proto 4 in {}ms", end_time)
        elif method_id == ProtoIdentifier.GET_ROUTES.value:
            logger.debug("Processing proto 1405 (GET_ROUTES)")
            await self.__process_routes(data["payload"], received_timestamp)
            end_time = self.get_time_ms() - start_time
            logger.debug("Done processing proto 1405 in {}ms", end_time)
        elif method_id == ProtoIdentifier.GYM_INFO.value:
            logger.debug("Processing proto 156 (GYM_GET_INFO)")
            gym_info: pogoprotos.GymGetInfoOutProto = pogoprotos.GymGetInfoOutProto()
            gym_info.ParseFromString(data["payload"])
            async with self.__db_wrapper as session, session:
                try:
                    await self.__db_submit.gym_info(session, gym_info)
                    await session.commit()
                except Exception as e:
                    logger.warning("Failed submitting gym info to DB: {}", e)

            end_time = self.get_time_ms() - start_time
            logger.debug("Done processing proto 156 in {}ms", end_time)
        else:
            logger.warning("Type {} was not processed as no processing is defined.", method_id)

    async def __process_lured_encounter(self, data: Dict, origin: str,
                                        processed_timestamp, received_timestamp, start_time_ms):
        playerlevel = await self.__mitm_mapper.get_level(origin)
        if MadGlobals.application_args.scan_lured_mons and (playerlevel >= 30):
            logger.debug("Processing lure encounter received at {}", processed_timestamp)
            encounter_proto: pogoprotos.DiskEncounterOutProto = ProtoHelper.parse(ProtoIdentifier.DISK_ENCOUNTER,
                                                                                  data["payload"])
            async with self.__db_wrapper as session, session:
                lure_encounter: Optional[Tuple[int, datetime]] = await self.__db_submit \
                    .mon_lure_iv(session, received_timestamp, encounter_proto)

                if MadGlobals.application_args.game_stats:
                    await self.__db_submit.update_seen_type_stats(session, lure_encounter=[lure_encounter])
                await session.commit()
            end_time = self.get_time_ms() - start_time_ms
            logger.debug("Done processing lure encounter in {}ms", end_time)

    async def __process_encounter(self, data: Dict, origin: str, received_date: datetime, received_timestamp: int,
                                  start_time_ms: int):
        encounter_proto: pogoprotos.EncounterOutProto = ProtoHelper.parse(ProtoIdentifier.ENCOUNTER, data["payload"])
        # TODO: Cache result in SerializedMitmDataProcessor to not spam the MITMMapper too much in that regard
        playerlevel = await self.__mitm_mapper.get_level(origin)
        if playerlevel >= 30:
            logger.debug("Processing encounter received at {}", received_date)
            async with self.__db_wrapper as session, session:
                encounter: Optional[Tuple[int, bool]] = await self.__db_submit.mon_iv(session,
                                                                                      received_timestamp,
                                                                                      encounter_proto)
            if MadGlobals.application_args.game_stats and encounter:
                encounter_id, is_shiny = encounter
                loop = asyncio.get_running_loop()
                loop.create_task(self.__stats_mon_iv(origin, encounter_id, received_date, is_shiny))
            end_time = self.get_time_ms() - start_time_ms
            logger.debug("Done processing encounter in {}ms", end_time)
        else:
            logger.warning("Playerlevel lower than 30 - not processing encounter IVs")

    async def __stats_mon_iv(self, origin: str, encounter_id: int, received_date: datetime, is_shiny: bool):
        await self.__stats_handler.stats_collect_mon_iv(origin, encounter_id, received_date, is_shiny)

    async def __process_gmo_raw(self, data: Dict, origin: str, received_date: datetime,
                                received_timestamp: int, start_time_ms: int):
        logger.debug("Processing GMO. Received at {}", received_date)
        # TODO: Offload conversion?
        gmo: pogoprotos.GetMapObjectsOutProto = ProtoHelper.parse(ProtoIdentifier.GMO, data["payload"])
        loop = asyncio.get_running_loop()
        weather_task = loop.create_task(self.__process_weather(gmo, received_timestamp))
        stops_task = loop.create_task(self.__process_stops(gmo))
        gyms_task = loop.create_task(self.__process_gyms(gmo, received_timestamp))
        raids_task = loop.create_task(self.__process_raids(gmo, received_timestamp))
        spawnpoints_task = loop.create_task(self.__process_spawnpoints(gmo, received_timestamp))
        cells_task = loop.create_task(self.__process_cells(gmo))
        mons_task = loop.create_task(self.__process_wild_mons(gmo, received_timestamp))
        stations_task = loop.create_task(self.__process_stations(gmo, received_timestamp))

        gmo_loc_start = self.get_time_ms()
        gmo_loc_time = self.get_time_ms() - gmo_loc_start
        lure_encounter_ids: List[int] = []
        lure_no_iv_task = None
        if MadGlobals.application_args.scan_lured_mons:
            lure_no_iv_task = loop.create_task(self.__process_lure_no_iv(gmo, received_timestamp))
        lure_processing_time = 0

        nearby_task = None
        if MadGlobals.application_args.scan_nearby_mons:
            nearby_task = loop.create_task(self.__process_nearby_mons(gmo, received_timestamp))
        nearby_cell_encounter_ids = []
        nearby_stop_encounter_ids = []
        nearby_mons_time = 0
        wild_encounter_ids_in_gmo, wild_mon_processing_time = await mons_task
        if nearby_task:
            nearby_cell_encounter_ids, nearby_stop_encounter_ids, nearby_mons_time = await nearby_task
        if lure_no_iv_task:
            lure_encounter_ids, lure_processing_time = await lure_no_iv_task

        weather_time = await weather_task
        raids_time, amount_raids = await raids_task
        spawnpoints_time = await spawnpoints_task
        cells_time = await cells_task
        stops_time = await stops_task
        gyms_time = await gyms_task
        stations_count_in_gmo, stations_time = await stations_task
        full_time = self.get_time_ms() - start_time_ms
        logger.debug("Done processing GMO in {}ms (weather={}ms, stops={}ms, gyms={}ms, raids={}ms[{}], " +
                     "spawnpoints={}ms, mons={}ms, "
                     "nearby_mons={}ms, lure_noiv={}ms, cells={}ms, stations={}ms[{}], "
                     "gmo_loc={}ms)",
                     full_time, weather_time, stops_time, gyms_time, raids_time, amount_raids,
                     spawnpoints_time, wild_mon_processing_time, nearby_mons_time, lure_processing_time,
                     cells_time, stations_time, stations_count_in_gmo, gmo_loc_time)
        # TODO: stats for stations_count_in_gmo?
        loop.create_task(self.__fire_stats_gmo_submission(origin, received_date,
                                                          wild_encounter_ids_in_gmo,
                                                          nearby_cell_encounter_ids,
                                                          nearby_stop_encounter_ids,
                                                          lure_encounter_ids,
                                                          amount_raids))

    async def __fire_stats_gmo_submission(self, worker: str, time_received_raw: datetime,
                                          wild_mon_encounter_ids_in_gmo: List[int],
                                          nearby_cell_mons: List[int],
                                          nearby_fort_mons: List[int],
                                          lure_mons: List[int],
                                          amount_raids: int):
        await self.__stats_handler.stats_collect_wild_mon(worker, wild_mon_encounter_ids_in_gmo, time_received_raw)
        await self.__stats_handler.stats_collect_seen_type(nearby_cell_mons, MonSeenTypes.nearby_cell,
                                                           time_received_raw)
        await self.__stats_handler.stats_collect_seen_type(nearby_fort_mons, MonSeenTypes.nearby_stop,
                                                           time_received_raw)
        await self.__stats_handler.stats_collect_seen_type(lure_mons, MonSeenTypes.lure_wild, time_received_raw)
        await self.__stats_handler.stats_collect_raid(worker, time_received_raw, amount_raids)

    async def __process_gmo_mon_stats(self, cell_encounters, lure_wild, stop_encounters, wild_encounter_ids_processed):
        async with self.__db_wrapper as session, session:
            await self.__db_submit.update_seen_type_stats(session,
                                                          wild=wild_encounter_ids_processed,
                                                          lure_wild=lure_wild,
                                                          nearby_cell=cell_encounters,
                                                          nearby_stop=stop_encounters)
            await session.commit()

    async def __process_lure_no_iv(self, gmo: pogoprotos.GetMapObjectsOutProto,
                                   received_timestamp: int) -> Tuple[List[int], int]:
        lurenoiv_start = self.get_time_ms()
        async with self.__db_wrapper as session, session:
            try:
                lure_wild = await self.__db_submit.mon_lure_noiv(session, received_timestamp, gmo)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting lure no iv: {}", e)
        lure_processing_time = self.get_time_ms() - lurenoiv_start
        return lure_wild, lure_processing_time

    async def __process_nearby_mons(self, gmo: pogoprotos.GetMapObjectsOutProto,
                                    received_timestamp: int) -> Tuple[List[int], List[int], int]:
        nearby_mons_time_start = self.get_time_ms()
        cell_encounters: List[int] = []
        stop_encounters: List[int] = []
        async with self.__db_wrapper as session, session:
            try:
                cell_encounters, stop_encounters = await self.__db_submit.mons_nearby(session, received_timestamp,
                                                                                      gmo)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting nearby mons: {}", e)
        nearby_mons_time = self.get_time_ms() - nearby_mons_time_start
        return cell_encounters, stop_encounters, nearby_mons_time

    async def __process_wild_mons(self, gmo: pogoprotos.GetMapObjectsOutProto,
                                  received_timestamp: int) -> Tuple[List[int], int]:
        mons_time_start = self.get_time_ms()
        encounter_ids_in_gmo: List[int] = []
        async with self.__db_wrapper as session, session:
            try:
                encounter_ids_in_gmo = await self.__db_submit.mons(session,
                                                                   received_timestamp,
                                                                   gmo)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting wild mons: {}", e)
        mons_time = self.get_time_ms() - mons_time_start
        return encounter_ids_in_gmo, mons_time

    async def __process_cells(self, data: pogoprotos.GetMapObjectsOutProto):
        cells_time_start = self.get_time_ms()
        async with self.__db_wrapper as session, session:
            try:
                await self.__db_submit.cells(session, data)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting cells: {}", e)
                await session.rollback()
        cells_time = self.get_time_ms() - cells_time_start
        return cells_time

    async def __process_spawnpoints(self, gmo: pogoprotos.GetMapObjectsOutProto, received_timestamp: int):
        spawnpoints_time_start = self.get_time_ms()
        async with self.__db_wrapper as session, session:
            try:
                await self.__db_submit.spawnpoints(session, gmo, received_timestamp)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting spawnpoints: {}", e)
        spawnpoints_time = self.get_time_ms() - spawnpoints_time_start
        return spawnpoints_time

    async def __process_raids(self, gmo: pogoprotos.GetMapObjectsOutProto, timestamp: int) -> Tuple[int, int]:
        """

        Args:
            gmo:
            timestamp:

        Returns: Tuple of duration taken to submit and amount of raids seen

        """
        raids_time_start = self.get_time_ms()
        amount_raids: int = 0
        async with self.__db_wrapper as session, session:
            try:
                amount_raids = await self.__db_submit.raids(session, gmo, timestamp)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting raids: {}", e)
        raids_time = self.get_time_ms() - raids_time_start
        return raids_time, amount_raids

    async def __process_routes(self, data: bytes, received_timestamp: int) -> None:
        routes_time_start = self.get_time_ms()
        routes: pogoprotos.GetRoutesOutProto = ProtoHelper.parse(ProtoIdentifier.GET_ROUTES, data)
        async with self.__db_wrapper as session, session:
            try:
                await self.__db_submit.routes(session, routes, received_timestamp)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting routes: {}", e)
                logger.exception(e)
        routes_time = self.get_time_ms() - routes_time_start
        logger.debug("Processing routes took {}ms", routes_time)

    async def __process_gyms(self, gmo: pogoprotos.GetMapObjectsOutProto, received_timestamp: int):
        gyms_time_start = self.get_time_ms()
        # TODO: If return value False, rollback transaction?
        async with self.__db_wrapper as session, session:
            try:
                await self.__db_submit.gyms(session, gmo, received_timestamp)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting gyms: {}", e)
        gyms_time = self.get_time_ms() - gyms_time_start
        return gyms_time

    async def __process_stops(self, gmo: pogoprotos.GetMapObjectsOutProto):
        stops_time_start = self.get_time_ms()
        async with self.__db_wrapper as session, session:
            try:
                await self.__db_submit.stops(session, gmo)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting stops: {}", e)
                logger.exception(e)
        stops_time = self.get_time_ms() - stops_time_start
        return stops_time

    async def __process_weather(self, gmo: pogoprotos.GetMapObjectsOutProto, received_timestamp: int):
        weather_time_start = self.get_time_ms()
        async with self.__db_wrapper as session, session:
            try:
                await self.__db_submit.weather(session, gmo, received_timestamp)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting weather: {}", e)
        weather_time = self.get_time_ms() - weather_time_start
        return weather_time

    @staticmethod
    def get_time_ms():
        return int(time.time() * 1000)

    async def _handle_inventory_data(self, origin: str, data: bytes) -> None:
        inventory_data: pogoprotos.GetHoloholoInventoryOutProto = ProtoHelper.parse(ProtoIdentifier.INVENTORY, data)
        if not inventory_data.inventory_delta:
            logger.debug2('gen_player_stats cannot generate new stats')
            return
        cache: Redis = await self.__db_wrapper.get_cache()
        cache_key: str = f"inv_data_{origin}_processed"
        if await cache.exists(cache_key):
            return
        stats: RepeatedCompositeFieldContainer[pogoprotos.InventoryItemProto] = inventory_data.inventory_delta.inventory_item
        if not stats:
            return
        for data_inventory in stats:
            player_stats: pogoprotos.PlayerStatsProto = data_inventory.inventory_item_data.player_stats
            if int(player_stats.level) > 0:
                logger.debug2('{{gen_player_stats}} saving new playerstats')
                if await self.__mitm_mapper.get_level(origin) != player_stats.level:
                    # Update the player level in DB...
                    async with self.__db_wrapper as session, session:
                        device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get_by_origin(
                            session, self.__db_wrapper.get_instance_id(), origin)
                        if device_entry:
                            await self.__account_handler.set_level(device_id=device_entry.device_id,
                                                                   level=player_stats.level)
                await self.__mitm_mapper.set_level(origin, int(player_stats.level))
                await self.__mitm_mapper.set_pokestop_visits(origin,
                                                             int(player_stats.poke_stop_visits))
                await cache.set(cache_key, int(time.time()), ex=60)
                return

    async def __process_stations(self, gmo: pogoprotos.GetMapObjectsOutProto,
                                  received_timestamp: int) -> Tuple[List[int], int]:
        stations_time_start = self.get_time_ms()
        async with self.__db_wrapper as session, session:
            try:
                stations_in_gmo = await self.__db_submit.stations(session,
                                                                   received_timestamp,
                                                                   gmo)
                await session.commit()
            except Exception as e:
                logger.warning("Failed submitting stations: {}", e)
        stations_time = self.get_time_ms() - stations_time_start
        return stations_in_gmo, stations_time
