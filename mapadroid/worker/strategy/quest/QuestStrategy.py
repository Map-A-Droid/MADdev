import asyncio
import math
import os
import time
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from loguru import logger
from s2sphere import CellId
from sqlalchemy import exc
from sqlalchemy.ext.asyncio import AsyncSession

import mapadroid.mitm_receiver.protos.Rpc_pb2 as pogoprotos
from mapadroid.account_handler import AbstractAccountHandler
from mapadroid.data_handler.mitm_data.AbstractMitmMapper import \
    AbstractMitmMapper
from mapadroid.data_handler.mitm_data.holder.latest_mitm_data.LatestMitmDataEntry import \
    LatestMitmDataEntry
from mapadroid.data_handler.stats.AbstractStatsHandler import \
    AbstractStatsHandler
from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.db.helper.PokestopHelper import PokestopHelper
from mapadroid.db.helper.SettingsPogoauthHelper import SettingsPogoauthHelper
from mapadroid.db.helper.TrsQuestHelper import TrsQuestHelper
from mapadroid.db.model import (Pokestop, SettingsAreaPokestop,
                                SettingsPogoauth, SettingsWalkerarea)
from mapadroid.mapping_manager.MappingManager import MappingManager
from mapadroid.mapping_manager.MappingManagerDevicemappingKey import \
    MappingManagerDevicemappingKey
from mapadroid.mitm_receiver.protos.ProtoHelper import ProtoHelper
from mapadroid.ocr.pogoWindows import PogoWindows
from mapadroid.ocr.screenPath import WordToScreenMatching
from mapadroid.utils.collections import Location, ScreenCoordinates
from mapadroid.utils.DatetimeWrapper import DatetimeWrapper
from mapadroid.utils.gamemechanicutil import (calculate_cooldown,
                                              determine_current_quest_layer)
from mapadroid.utils.geo import get_distance_of_two_points_in_meters
from mapadroid.utils.global_variables import (MIN_LEVEL_IV,
                                              QUEST_WALK_SPEED_CALCULATED)
from mapadroid.utils.madConstants import (FALLBACK_MITM_WAIT_TIMEOUT,
                                          STOP_SPIN_DISTANCE, TIMESTAMP_NEVER)
from mapadroid.utils.madGlobals import (FortSearchResultTypes,
                                        InternalStopWorkerException,
                                        QuestLayer, TransportType)
from mapadroid.utils.ProtoIdentifier import ProtoIdentifier
from mapadroid.utils.s2Helper import S2Helper
from mapadroid.websocket.AbstractCommunicator import AbstractCommunicator
from mapadroid.worker.ReceivedTypeEnum import ReceivedType
from mapadroid.worker.strategy.AbstractMitmBaseStrategy import \
    AbstractMitmBaseStrategy
from mapadroid.worker.WorkerState import WorkerState

# The diff to lat/lng values to consider that the worker is standing on top of the stop
S2_GMO_CELL_LEVEL = 15
RADIUS_FOR_CELLS_CONSIDERED_FOR_STOP_SCAN = 30
DISTANCE_TO_STOP_TO_CONSIDER_ON_TOP = 0.00006


class AbortStopProcessingException(Exception):
    pass


class PositionStopType(Enum):
    GMO_NOT_AVAILABLE = 0,
    GMO_EMPTY = 1,
    GYM = 2,
    VISITED_STOP_IN_LEVEL_MODE_TO_IGNORE = 3,
    STOP_DISABLED = 4,
    STOP_CLOSED = 5,
    STOP_COOLDOWN = 6
    NO_FORT = 7
    SPINNABLE_STOP = 8

    @staticmethod
    def type_contains_stop_at_all(position_stop_type) -> bool:
        return position_stop_type in (PositionStopType.SPINNABLE_STOP,
                                      PositionStopType.VISITED_STOP_IN_LEVEL_MODE_TO_IGNORE,
                                      PositionStopType.STOP_CLOSED, PositionStopType.STOP_COOLDOWN,
                                      PositionStopType.STOP_DISABLED)


class QuestStrategy(AbstractMitmBaseStrategy, ABC):
    def __init__(self, area_id: int, communicator: AbstractCommunicator,
                 mapping_manager: MappingManager,
                 db_wrapper: DbWrapper, word_to_screen_matching: WordToScreenMatching,
                 pogo_windows_handler: PogoWindows,
                 walker: SettingsWalkerarea,
                 worker_state: WorkerState,
                 mitm_mapper: AbstractMitmMapper,
                 stats_handler: AbstractStatsHandler,
                 quest_layer_to_scan: QuestLayer,
                 account_handler: AbstractAccountHandler):
        super().__init__(area_id=area_id,
                         communicator=communicator, mapping_manager=mapping_manager,
                         db_wrapper=db_wrapper,
                         word_to_screen_matching=word_to_screen_matching,
                         pogo_windows_handler=pogo_windows_handler,
                         walker=walker,
                         worker_state=worker_state,
                         mitm_mapper=mitm_mapper,
                         stats_handler=stats_handler,
                         account_handler=account_handler)
        self._ready_for_scan: asyncio.Event = asyncio.Event()

        self._spinnable_data_failcount = 0
        self._always_cleanup: bool = False
        self._rotation_waittime: int = 0
        self._clustering_enabled: bool = False
        self._ignore_spinned_stops: bool = False
        # TODO: Move to worker_state?
        self._delay_add: int = 0
        self._stop_process_time: int = TIMESTAMP_NEVER
        self._quest_layer_to_scan: QuestLayer = quest_layer_to_scan

    async def _check_for_data_content(self, latest: Optional[LatestMitmDataEntry],
                                      proto_to_wait_for: ProtoIdentifier,
                                      timestamp: int) -> Tuple[ReceivedType, Optional[Any]]:
        type_of_data_found: ReceivedType = ReceivedType.UNDEFINED
        data_found: Optional[Any] = None

        # proto has previously been received, let's check the timestamp...
        if not latest:
            logger.debug("No data linked to the requested proto since MAD started.")
            return type_of_data_found, data_found
        timestamp_of_proto = latest.timestamp_of_data_retrieval
        if not timestamp_of_proto or timestamp_of_proto < timestamp:
            logger.debug("latest timestamp of proto {} ({}) is older than {}", proto_to_wait_for,
                         timestamp_of_proto, timestamp)
            # TODO: timeout error instead of data_error_counter? Differentiate timeout vs missing data (the
            # TODO: latter indicates too high speeds for example
            return type_of_data_found, data_found

        # TODO: consider resetting timestamp here since we clearly received SOMETHING
        latest_proto: Union[List, Dict, bytes] = latest.data
        logger.debug4("Latest data received: {}", latest_proto)
        if latest_proto is None:
            logger.debug("No proto data for {} at {} after {}", proto_to_wait_for,
                         timestamp_of_proto, timestamp)
            return type_of_data_found, data_found
        logger.debug2("Checking for Quest related data in proto {}", proto_to_wait_for)
        if proto_to_wait_for == ProtoIdentifier.FORT_SEARCH:
            fort_search: pogoprotos.FortSearchOutProto = ProtoHelper.parse(ProtoIdentifier.FORT_SEARCH, latest_proto)
            # TODO..
            quest_type: int = fort_search.challenge_quest.quest.quest_type
            result: int = fort_search.result
            amount_items_awarded: int = len(fort_search.items)
            if result == 1 and amount_items_awarded == 0:
                return ReceivedType.FORT_SEARCH_RESULT, FortSearchResultTypes.TIME
            elif result == 1 and quest_type == 0:
                return ReceivedType.FORT_SEARCH_RESULT, FortSearchResultTypes.FULL
            elif result == 1 and amount_items_awarded > 0:
                return ReceivedType.FORT_SEARCH_RESULT, FortSearchResultTypes.QUEST
            elif result == 2:
                return ReceivedType.FORT_SEARCH_RESULT, FortSearchResultTypes.OUT_OF_RANGE
            elif result == 3:
                return ReceivedType.FORT_SEARCH_RESULT, FortSearchResultTypes.COOLDOWN
            elif result == 4:
                return ReceivedType.FORT_SEARCH_RESULT, FortSearchResultTypes.INVENTORY
            elif result == 5:
                return ReceivedType.FORT_SEARCH_RESULT, FortSearchResultTypes.LIMIT
        elif proto_to_wait_for == ProtoIdentifier.FORT_DETAILS:
            fort_details: pogoprotos.FortDetailsOutProto = ProtoHelper.parse(ProtoIdentifier.FORT_DETAILS, latest_proto)
            fort_type: int = fort_details.fort_type
            data_found = fort_details
            type_of_data_found = ReceivedType.GYM if fort_type == 0 else ReceivedType.STOP
        elif proto_to_wait_for == ProtoIdentifier.GMO:
            gmo: pogoprotos.GetMapObjectsOutProto = ProtoHelper.parse(ProtoIdentifier.GMO, latest_proto)
            if self._directly_surrounding_gmo_cells_containing_stops_around_current_position(gmo):
                data_found = latest_proto
                type_of_data_found = ReceivedType.GMO

        return type_of_data_found, data_found

    async def pre_work_loop(self):
        await super().pre_work_loop()
        if self._worker_state.stop_worker_event.is_set() or not await self._wait_for_injection():
            raise InternalStopWorkerException("Worker is supposed to be stopped while working waiting for injection")

        await self.pre_work_loop_layer_preparation()

    @abstractmethod
    async def pre_work_loop_layer_preparation(self) -> None:
        """
        Operation to run before any scan is started. E.g., ensuring no AR quest is present in inventory upon starting
        layer 0 (AR) scanning
        Check currently held quests -> Mode
        If it does not match the mode desired, open the quest menu to trigger cleanup accordingly.
        However, if a quest is needed to scan the desired layer, quests and locations need to be ignored until the
        necessary mode has been reached...
        Returns:

        """
        pass

    async def get_current_layer_of_worker(self) -> QuestLayer:
        """

        Returns:
        Raises: ValueError if the layer cannot be determined yet
        """
        quests_held: Optional[List[int]] = await self._mitm_mapper.get_quests_held(self._worker_state.origin)
        return determine_current_quest_layer(quests_held)

    @abstractmethod
    async def _check_layer(self) -> None:
        """
        Update self._ready_for_scan as needed
        Returns:

        """
        pass

    async def move_to_location(self):
        distance, area_settings = await self._get_route_manager_settings_and_distance_to_current_location()
        area_settings: SettingsAreaPokestop = area_settings
        logger.debug("Getting time")
        if (not area_settings.speed or area_settings.speed == 0 or
                (area_settings.max_distance and 0 < area_settings.max_distance < distance)
                or (self._worker_state.last_location.lat == 0.0 and self._worker_state.last_location.lng == 0.0)):
            logger.debug("main: Teleporting...")
            self._worker_state.last_transport_type = TransportType.TELEPORT
            await self._communicator.set_location(
                Location(self._worker_state.current_location.lat, self._worker_state.current_location.lng), 0)
            # the time we will take as a starting point to wait for data...
            cur_time = math.floor(time.time())

            delay_used = await self.get_devicesettings_value(MappingManagerDevicemappingKey.POST_TELEPORT_DELAY, 0)
            walk_distance_post_teleport = await self.get_devicesettings_value(
                MappingManagerDevicemappingKey.WALK_AFTER_TELEPORT_DISTANCE, 0)
            if 0 < walk_distance_post_teleport < distance:
                to_walk = await self._walk_after_teleport(walk_distance_post_teleport)
                delay_used -= (to_walk / 3.05) - 1.  # We already waited for a bit because of this walking part
                if delay_used < 0:
                    delay_used = 0
        else:
            time_it_takes_to_walk = distance / (area_settings.speed / 3.6)  # speed is in kmph , delay_used need mps
            logger.info("main: Walking {} m, this will take {} seconds", distance, time_it_takes_to_walk)
            await self._mapping_manager.routemanager_set_worker_sleeping(self._area_id,
                                                                         self._worker_state.origin,
                                                                         time_it_takes_to_walk)
            cur_time = await self._walk_to_location(area_settings.speed)
            delay_used = await self.get_devicesettings_value(MappingManagerDevicemappingKey.POST_WALK_DELAY, 0)

        # Calculate distance to the previous location and wait for the time needed. This also applies to "walking"
        #  since walking above a given speed may result in softbans as well
        delay_to_avoid_softban: int = 0
        delay_to_avoid_softban, distance = await self._calculate_remaining_softban_avoidance_duration(cur_time,
                                                                                                      delay_to_avoid_softban,
                                                                                                      distance,
                                                                                                      QUEST_WALK_SPEED_CALCULATED)
        if delay_to_avoid_softban == 0:
            last_action_time: Optional[int] = await self.get_devicesettings_value(MappingManagerDevicemappingKey
                                                                                  .LAST_ACTION_TIME, None)
            if last_action_time and last_action_time > 0:
                timediff = time.time() - last_action_time
                logger.info("Timediff between now and last action time: {}", int(timediff))
                delay_to_avoid_softban = delay_used - timediff
            elif self._worker_state.last_location.lat == 0.0 and self._worker_state.last_location.lng == 0.0:
                logger.info('Starting fresh round - using lower delay')
                # Take into account trs_status possible softban
            else:
                delay_to_avoid_softban = calculate_cooldown(distance, QUEST_WALK_SPEED_CALCULATED)
        if delay_to_avoid_softban > 0 and delay_to_avoid_softban > delay_used:
            delay_used = delay_to_avoid_softban

        if delay_used > 0:
            logger.debug(
                "Need more sleep after moving to the new location: {} seconds!", int(delay_used))
        delay_used = await self._rotate_account_after_moving_locations_if_applicable(delay_used)

        delay_used = math.floor(delay_used)
        if delay_used <= 0:
            self._worker_state.current_sleep_duration = 0
            logger.info('No need to wait before spinning, continuing...')
        else:
            logger.info("Real sleep time: {} seconds: next action {}", delay_used,
                        DatetimeWrapper.now() + timedelta(seconds=delay_used))
            self._worker_state.current_sleep_duration = delay_used
            await self.worker_stats()

            await self._mapping_manager.routemanager_set_worker_sleeping(self._area_id,
                                                                         self._worker_state.origin,
                                                                         delay_used)
            while time.time() <= int(cur_time) + int(delay_used):
                if not await self._mapping_manager.routemanager_present(self._area_id) \
                        or self._worker_state.stop_worker_event.is_set():
                    logger.error("Worker was killed while sleeping")
                    self._worker_state.current_sleep_duration = 0
                    raise InternalStopWorkerException("Worker has been removed from routemanager or is supposed to stop"
                                                      "during the move to a location")
                await asyncio.sleep(1)

            # subtract up to 10s if the delay was less than that or bigger
            if delay_used > 10:
                cur_time -= 10
            elif delay_used > 0:
                cur_time -= delay_used

        self._worker_state.current_sleep_duration = 0
        await self.set_devicesettings_value(MappingManagerDevicemappingKey.LAST_LOCATION,
                                            self._worker_state.current_location)
        self._worker_state.last_location = self._worker_state.current_location
        return cur_time

    async def _calculate_remaining_softban_avoidance_duration(self, cur_time, delay_to_avoid_softban, distance, speed):
        async with self._db_wrapper as session, session:
            active_account: Optional[SettingsPogoauth] = await SettingsPogoauthHelper.get_assigned_to_device(
                session, self._worker_state.device_id)
            if active_account:
                logger.debug("Checking DB for last softban action")
                if active_account.last_softban_action \
                        and active_account.last_softban_action_location:
                    logger.debug("Last softban action at {} took place at {}",
                                 active_account.last_softban_action,
                                 active_account.last_softban_action_location)
                    last_action_location: Location = Location(active_account.last_softban_action_location[0],
                                                              active_account.last_softban_action_location[1])
                    distance_last_action = get_distance_of_two_points_in_meters(last_action_location.lat,
                                                                                last_action_location.lng,
                                                                                self._worker_state.current_location.lat,
                                                                                self._worker_state.current_location.lng)
                    delay_to_last_action = calculate_cooldown(distance_last_action, speed)
                    logger.debug("Last registered softban was at {} at {}", active_account.last_softban_action,
                                 active_account.last_softban_action_location)
                    if active_account.last_softban_action.timestamp() + delay_to_last_action > cur_time:
                        logger.debug("Last registered softban requires further cooldown")
                        delay_to_avoid_softban = max(0.0, delay_to_last_action
                                                     - (cur_time - active_account.last_softban_action.timestamp()))
                        distance = distance_last_action
                    else:
                        logger.debug("Last registered softban action long enough in the past")
                else:
                    logger.warning("No last softban action known for active account ({})",
                                   active_account.account_id)
            else:
                logger.warning("Missing assignment of pogoauth to device {} ({}) or no last known softban action",
                               self._worker_state.origin, self._worker_state.device_id)
        return delay_to_avoid_softban, distance

    async def _rotate_account_after_moving_locations_if_applicable(self, delay_used: int) -> int:
        if await self.get_devicesettings_value(MappingManagerDevicemappingKey.SCREENDETECTION, True) and \
                delay_used >= self._rotation_waittime \
                and await self.get_devicesettings_value(MappingManagerDevicemappingKey.ACCOUNT_ROTATION,
                                                        False) and not await self._is_levelmode():
            # Waiting time too long and more than one account - switch! (not level mode!!)
            logger.info('Can use more than 1 account - switch & no cooldown')
            await self.switch_account()
            delay_used = -1
        elif await self._is_levelmode() and await self._mitm_mapper.get_level(
                self._worker_state.origin) >= MIN_LEVEL_IV:
            logger.info('Levelmode: Account of {} is level {}, i.e., >= {}, switching to next to level',
                        self._worker_state.origin,
                        await self._mitm_mapper.get_level(self._worker_state.origin) >= MIN_LEVEL_IV, MIN_LEVEL_IV)
            await self.switch_account()
            delay_used = -1
        return delay_used

    async def _is_levelmode(self):
        return await self._mapping_manager.routemanager_is_levelmode(self._area_id)

    async def post_move_location_routine(self, timestamp):
        if self._worker_state.stop_worker_event.is_set():
            raise InternalStopWorkerException("Worker is supposed to stop, aborting post_move_location_routine")
        await self._check_position_type()
        await self._switch_account_if_needed()
        await self._process_stop_at_location(timestamp)

    async def _process_stop_at_location(self, timestamp):
        logger.info("Processing Stop / Quest...")
        try:
            stop_type_present: PositionStopType = await self._ensure_stop_present(timestamp)
            if stop_type_present == PositionStopType.STOP_COOLDOWN:
                logger.warning("Stops at current position are on cooldown, move on")
                # TODO: Get all stops surrounding the current position, fetch quests, get latest timestamp which
                #  most likely is the one we need to use for cooldown calculations
            elif stop_type_present == PositionStopType.SPINNABLE_STOP:
                logger.info('Open Stop')
                await self._handle_stop(timestamp)
            else:
                logger.warning("No spinnable stop at the current location, aborting.")
                return
            await self.set_devicesettings_value(MappingManagerDevicemappingKey.LAST_ACTION_TIME, time.time())
        except AbortStopProcessingException as e:
            # The stop cannot be processed for whatever reason.
            # Stop processing the location.
            logger.warning("Failed handling stop(s) at {}: {}", self._worker_state.current_location, e)
            return
        finally:
            try:
                await self._check_layer()
            except ValueError as e:
                pass
            if not self._ready_for_scan.is_set():
                # Return location to the routemanager to be considered for a scan later on
                # TODO: What if the route is too small to fetch any useful data needed to scan the layer?...
                await self._mapping_manager.routemanager_redo_stop_at_end(self.area_id,
                                                                          self._worker_state.origin,
                                                                          self._worker_state.current_location)

    async def _check_position_type(self):
        position_type = await self._mapping_manager.routemanager_get_position_type(self._area_id,
                                                                                   self._worker_state.origin)
        if position_type is None:
            logger.warning("Mappings/Routemanagers have changed, stopping worker to be created again")
            raise InternalStopWorkerException("Mappings/Routemanagers have changed, stopping worker")

    async def _switch_account_if_needed(self):
        if await self.get_devicesettings_value(MappingManagerDevicemappingKey.ROTATE_ON_LVL_30, False) \
                and await self._mitm_mapper.get_level(self._worker_state.origin) >= 30 \
                and await self._is_levelmode():
            # switch if player lvl >= 30
            await self.switch_account()

    async def worker_specific_setup_start(self):
        area_settings: Optional[SettingsAreaPokestop] = await self._mapping_manager.routemanager_get_settings(
            self._area_id)
        self._rotation_waittime = await self.get_devicesettings_value(MappingManagerDevicemappingKey.ROTATION_WAITTIME,
                                                                      300)
        self._always_cleanup: bool = False if area_settings.cleanup_every_spin == 0 else True
        self._delay_add = int(await self.get_devicesettings_value(MappingManagerDevicemappingKey.VPS_DELAY, 0))
        self._ignore_spinned_stops: bool = area_settings.ignore_spinned_stops \
            if area_settings.ignore_spinned_stops or area_settings.ignore_spinned_stops is None else False

    async def worker_specific_setup_stop(self):
        pass

    async def _check_pogo_button(self):
        logger.debug("checkPogoButton: Trying to find buttons")
        pogo_topmost = await self._communicator.is_pogo_topmost()
        if not pogo_topmost:
            return False
        if not await self._take_screenshot(
                delay_before=await self.get_devicesettings_value(MappingManagerDevicemappingKey.POST_SCREENSHOT_DELAY,
                                                                 1)):
            logger.debug("checkPogoButton: Failed getting screenshot")
            return False
        # TODO: os operation asyncio?
        if os.path.isdir(await self.get_screenshot_path()):
            logger.error("checkPogoButton: screenshot.png is not a file/corrupted")
            return False

        logger.debug("checkPogoButton: checking for buttons")
        # TODO: need to be non-blocking
        found: bool = False
        coordinates: Optional[ScreenCoordinates] = await self._pogo_windows_handler \
            .look_for_button(await self.get_screenshot_path(), 2.20, 3.01)
        if coordinates:
            await self._communicator.click(coordinates.x, coordinates.y)
            await asyncio.sleep(1)
            logger.debug("checkPogoButton: Found button (small)")
        else:
            coordinates: Optional[ScreenCoordinates] = await self._pogo_windows_handler \
                .look_for_button(await self.get_screenshot_path(), 1.05, 2.20)
            if coordinates:
                await self._communicator.click(coordinates.x, coordinates.y)
                await asyncio.sleep(1)
                logger.debug("checkPogoButton: Found button (big)")
                found = True
        logger.debug("checkPogoButton: done")
        return found

    async def _check_pogo_close(self, takescreen=True):
        logger.debug("checkPogoClose: Trying to find closeX")
        if not await self._communicator.is_pogo_topmost():
            return False

        if takescreen:
            if not await self._take_screenshot(delay_before=await self.get_devicesettings_value(
                    MappingManagerDevicemappingKey.POST_SCREENSHOT_DELAY, 1)):
                logger.debug("checkPogoClose: Could not get screenshot")
                return False

        # TODO: Async...
        if os.path.isdir(await self.get_screenshot_path()):
            logger.error("checkPogoClose: screenshot.png is not a file/corrupted")
            return False

        logger.debug("checkPogoClose: checking for CloseX")
        found = await self._pogo_windows_handler.check_close_except_nearby_button(await self.get_screenshot_path(),
                                                                                  self._worker_state.origin)
        if found:
            await self._communicator.click(found[0].x, found[0].y)
            await asyncio.sleep(1)
            logger.debug("checkPogoClose: Found (X) button (except nearby)")
            logger.debug("checkPogoClose: done")
            return True
        logger.debug("checkPogoClose: done")
        return False

    async def switch_account(self):
        if not await self._switch_user():
            logger.error('Something happened while account switching :(')
            raise InternalStopWorkerException("Failed switching accounts")
        else:
            await asyncio.sleep(10)
            reached_main_menu = await self._check_pogo_main_screen(10, True)
            if not reached_main_menu:
                if not await self._restart_pogo():
                    # TODO: put in loop, count up for a reboot ;)
                    raise InternalStopWorkerException("Failed reaching the pogo main screen after switching accounts")

    async def _get_ids_iv_and_scanmode(self) -> Tuple[List[int], str]:
        injected_settings = {}
        scanmode = "quests"
        injected_settings["scanmode"] = scanmode
        ids_iv: List[int] = []
        self._encounter_ids = {}
        return ids_iv, scanmode

    async def _wait_for_data_after_moving(self, timestamp: float, proto_to_wait_for: ProtoIdentifier, timeout) \
            -> Tuple[ReceivedType, Optional[Any], float]:
        try:
            timeout_default: int = await self.get_devicesettings_value(MappingManagerDevicemappingKey.MITM_WAIT_TIMEOUT,
                                                                       FALLBACK_MITM_WAIT_TIMEOUT)
            if timeout_default > timeout:
                timeout = timeout_default
            return await asyncio.wait_for(self._wait_for_data(timestamp=timestamp,
                                                              proto_to_wait_for=proto_to_wait_for,
                                                              timeout=timeout), timeout)
        except asyncio.TimeoutError as e:
            logger.warning("Failed fetching data {} in {} seconds", proto_to_wait_for, timeout)
            return ReceivedType.UNDEFINED, None, 0.0

    async def _ensure_stop_present(self, timestamp: float) -> PositionStopType:
        # let's first check the GMO for the stop we intend to visit and abort if it's disabled, a gym, whatsoever
        logger.debug("Checking whether a stop is found in a GMO after {}", timestamp)
        stop_type: PositionStopType = await self._current_position_has_spinnable_stop(timestamp)
        if await self._is_levelmode():
            logger.info("Wait for new data to check stop present")
            if stop_type in (PositionStopType.GMO_NOT_AVAILABLE, PositionStopType.GMO_EMPTY,
                             PositionStopType.NO_FORT):
                raise AbortStopProcessingException("No fort present or GMO empty, continuing in levelmode.")
        else:
            if stop_type in (PositionStopType.GMO_NOT_AVAILABLE, PositionStopType.GMO_EMPTY):
                # Since GMOs are checked in wait_for_data (and _spinnable_data_failure) and consecutive timeouts
                #  will trigger device/pogo reboots,
                #  we simply append the current stop to the end of the route to check it again.
                logger.info("GMO invalid for current position, appending current location to the end of the route to "
                            "check it again.")
                await self._mapping_manager.routemanager_redo_stop_at_end(self._area_id,
                                                                          self._worker_state.origin,
                                                                          self._worker_state.current_location)
                timestamp = int(time.time())
                stop_type: PositionStopType = await self._current_position_has_spinnable_stop(timestamp)

        if not PositionStopType.type_contains_stop_at_all(stop_type):
            logger.info("Location {}, {} considered to be ignored in the next round due to failed "
                        "spinnable check",
                        self._worker_state.current_location.lat,
                        self._worker_state.current_location.lng)
            await self._mapping_manager.routemanager_add_coords_to_be_removed(self._area_id,
                                                                              self._worker_state.current_location.lat,
                                                                              self._worker_state.current_location.lng)
            raise AbortStopProcessingException("Stop not present")
        elif stop_type in (PositionStopType.STOP_CLOSED, PositionStopType.STOP_DISABLED):
            logger.info("Stop at {}, {} cannot be spun at the moment ({})",
                        self._worker_state.current_location.lat,
                        self._worker_state.current_location.lng,
                        stop_type)
            # TODO: Count up to a certain threshold to remove
            await self._mapping_manager.routemanager_add_coords_to_be_removed(self._area_id,
                                                                              self._worker_state.current_location.lat,
                                                                              self._worker_state.current_location.lng)
            raise AbortStopProcessingException("Stop cannot be spun at the moment")
        elif stop_type == PositionStopType.STOP_COOLDOWN:
            logger.info("Stop at {}, {} assumed to be spun already, we got cooldown ({})",
                        self._worker_state.current_location.lat,
                        self._worker_state.current_location.lng,
                        stop_type)
        elif stop_type == PositionStopType.VISITED_STOP_IN_LEVEL_MODE_TO_IGNORE:
            logger.info("Stop at {}, {} has been spun before and is to be ignored in the next round.")
            await self._mapping_manager.routemanager_add_coords_to_be_removed(self._area_id,
                                                                              self._worker_state.current_location.lat,
                                                                              self._worker_state.current_location.lng)
            raise AbortStopProcessingException("Stop has been spun before but levelmode is active")
        return stop_type

    async def _current_position_has_spinnable_stop(self, timestamp: float) -> PositionStopType:
        type_received, data_received, time_received = await self._wait_for_data_after_moving(timestamp,
                                                                                             ProtoIdentifier.GMO, 35)
        if type_received != ReceivedType.GMO or data_received is None:
            await self._spinnable_data_failure()
            return PositionStopType.GMO_NOT_AVAILABLE
        latest_proto: pogoprotos.GetMapObjectsOutProto = ProtoHelper.parse(ProtoIdentifier.GMO, data_received)

        if not latest_proto.map_cell:
            logger.warning("Can't spin stop - no map info in GMO!")
            await self._spinnable_data_failure()
            return PositionStopType.GMO_EMPTY

        distance_to_consider_for_stops = STOP_SPIN_DISTANCE
        cells_with_stops: List[
            pogoprotos.ClientMapCellProto] = self._directly_surrounding_gmo_cells_containing_stops_around_current_position(latest_proto)
        stop_types: Set[PositionStopType] = set()
        async with self._db_wrapper as session, session:
            for cell in cells_with_stops:
                forts: RepeatedCompositeFieldContainer[pogoprotos.PokemonFortProto] = cell.fort
                if not forts:
                    continue

                for fort in forts:
                    latitude: float = fort.latitude
                    longitude: float = fort.longitude
                    # TODO: Ensure this and the following IF works
                    fort_type: int = fort.fort_type

                    if latitude == 0.0 or longitude == 0.0 or fort_type == 0:
                        # invalid location or fort is a gym
                        continue

                    elif get_distance_of_two_points_in_meters(latitude, longitude,
                                                              self._worker_state.current_location.lat,
                                                              self._worker_state.current_location.lng) \
                            < distance_to_consider_for_stops:
                        # We are basically on top of a stop
                        logger.info("Found stop/gym at current location!")
                    else:
                        logger.debug2(
                            "Found stop nearby but not next to us to be spinned. Current lat, lng: {}, {}."
                            "Stop at {}, {}",
                            self._worker_state.current_location.lat,
                            self._worker_state.current_location.lng,
                            latitude, longitude)
                        continue

                    if await self._is_levelmode() and self._ignore_spinned_stops and fort.visited:
                        logger.info("Level mode: Stop already visited - skipping it")
                        self._spinnable_data_failcount = 0
                        stop_types.add(PositionStopType.VISITED_STOP_IN_LEVEL_MODE_TO_IGNORE)
                        continue

                    if not fort.enabled:
                        logger.info("Can't spin the stop - it is disabled")
                        stop_types.add(PositionStopType.STOP_DISABLED)
                        continue
                    if fort.closed:
                        logger.info("Can't spin the stop - it is closed")
                        stop_types.add(PositionStopType.STOP_CLOSED)
                        continue

                    if not fort.cooldown_complete_ms == 0:
                        logger.info("Can't spin the stop - it has cooldown, it has been spun already.")
                        stop_types.add(PositionStopType.STOP_COOLDOWN)
                        self._spinnable_data_failcount = 0
                        continue
                    self._spinnable_data_failcount = 0
                    stop_types.add(PositionStopType.SPINNABLE_STOP)
            if not stop_types:
                # by now we should've found the stop in the GMO
                logger.warning("Unable to confirm the current location ({}) yielding a spinnable stop "
                               "- likely not standing exactly on top ...",
                               self._worker_state.current_location)
                await self._check_if_stop_was_nearby_and_update_location(session, latest_proto)
                await self._spinnable_data_failure()
                try:
                    await session.commit()
                except Exception as e:
                    logger.exception(e)
                    await session.rollback()
                return PositionStopType.NO_FORT
            if len(stop_types) == 1:
                return stop_types.pop()
            elif PositionStopType.SPINNABLE_STOP in stop_types:
                return PositionStopType.SPINNABLE_STOP
            elif PositionStopType.STOP_COOLDOWN in stop_types:
                return PositionStopType.STOP_COOLDOWN
            elif PositionStopType.STOP_CLOSED in stop_types:
                return PositionStopType.STOP_CLOSED
            elif PositionStopType.STOP_DISABLED in stop_types:
                return PositionStopType.STOP_DISABLED
            elif PositionStopType.VISITED_STOP_IN_LEVEL_MODE_TO_IGNORE in stop_types:
                return PositionStopType.VISITED_STOP_IN_LEVEL_MODE_TO_IGNORE
            else:
                # More than one stop and various outcomes, just pop one...
                return stop_types.pop()

    async def _spinnable_data_failure(self):
        if self._spinnable_data_failcount > 3:
            self._spinnable_data_failcount = 0
            logger.warning("Worker failed spinning stop with GMO/data issues 3+ times - restart pogo")
            if not await self._restart_pogo() and not await self._reboot():
                raise InternalStopWorkerException("Failed restarting pogo as well as rebooting device after "
                                                  "attempting to spin a stop")
        else:
            self._spinnable_data_failcount += 1

    def _directly_surrounding_gmo_cells_containing_stops_around_current_position(
            self, gmo: pogoprotos.GetMapObjectsOutProto) -> List[pogoprotos.ClientMapCellProto]:
        """
        Returns a list of cells containing forts
        Args:
            gmo:

        Returns:
            List of cells that actually contain forts around the current position
        """
        cells_with_forts: List[pogoprotos.ClientMapCellProto] = []
        if not gmo.map_cell:
            logger.debug("No GMO cells passed for surrounding cell check")
            return cells_with_forts
        # 35m radius around current location (thus cells that may be touched by that radius hopefully get included)
        s2cells_valid_around_location: List[CellId] = \
            S2Helper.get_s2cells_from_circle(self._worker_state.current_location.lat,
                                             self._worker_state.current_location.lng,
                                             RADIUS_FOR_CELLS_CONSIDERED_FOR_STOP_SCAN,
                                             S2_GMO_CELL_LEVEL)
        s2cell_ids_valid: List[str] = [s2cell.id() for s2cell in s2cells_valid_around_location]
        for cell in gmo.map_cell:
            # each cell contains an array of forts, check each cell for a fort with our current location (maybe +-
            # very very little jitter) and check its properties
            if cell.s2_cell_id not in s2cell_ids_valid:
                continue
            forts: RepeatedCompositeFieldContainer[pogoprotos.PokemonFortProto] = cell.fort
            if forts:
                cells_with_forts.append(cell)

        if not cells_with_forts:
            logger.debug2("GMO cells around current position ({}) do not contain stops ",
                          self._worker_state.current_location)
        return cells_with_forts

    async def _check_if_stop_was_nearby_and_update_location(self, session: AsyncSession,
                                                            gmo: pogoprotos.GetMapObjectsOutProto) -> None:
        logger.info("Checking stops around current location ({}) for deleted stops.",
                    self._worker_state.current_location)

        stops: Dict[str, Pokestop] = await PokestopHelper.get_nearby(session, self._worker_state.current_location)
        logger.debug("Checking if GMO contains location changes or DB has stops that are already deleted. In DB: "
                     "{}. GMO cells: {}", str(stops), gmo.map_cell)
        # TODO: str()?
        # stops may contain multiple stops now. We can check each ID (key of dict) with the IDs in the GMO.
        # Then cross check against the location. If that differs, we need to update/delete the entries in the DB
        for cell in gmo.map_cell:
            forts: RepeatedCompositeFieldContainer[pogoprotos.PokemonFortProto] = cell.fort
            if not forts:
                continue

            for fort in forts:
                if not fort.latitude or not fort.longitude or not fort.fort_id:
                    logger.warning("Cannot process fort without id, lat or lon")
                    continue
                stop: Optional[Pokestop] = stops.get(fort.fort_id)
                if stop is None:
                    # new stop we have not seen before, MITM processors should take care of that
                    logger.debug2("Stop not in DB (in range) with ID {} at {}, {}",
                                  fort.fort_id, fort.latitude, fort.longitude)
                    continue
                else:
                    stops.pop(fort.fort_id)
                # TODO: Is this comparison too strict?
                if float(stop.latitude) == fort.latitude and float(stop.longitude) == fort.longitude:
                    # Location of fort has not changed
                    # logger.debug2("Fort {} has not moved", fort_id)
                    continue
                else:
                    # now we have a location from DB for the given stop we are currently processing but does not equal
                    # the one we are currently processing, thus SAME fort_id
                    # now update the stop
                    logger.warning("Updating fort {} with previous location {}, {} now placed at {}, {}",
                                   fort.fort_id, stop.latitude, stop.longitude, fort.latitude, fort.longitude)
                    async with self._db_wrapper as fresh_session, fresh_session:
                        await PokestopHelper.update_location(fresh_session, fort.fort_id,
                                                             Location(fort.latitude, fort.longitude))
                        try:
                            await fresh_session.commit()
                        except exc.InternalError as e:
                            logger.warning("Failed updating location of stop")
                            logger.exception(e)
                            await fresh_session.rollback()

        timedelta_to_consider_deletion = timedelta(days=3)
        for fort_id, stop in stops.items():
            # Call delete of stops that have been not been found within 100m range of current position
            stop_location: Location = Location(float(stop.latitude), float(stop.longitude))
            logger.debug("Considering stop {} at {} (last updated {}) for deletion",
                         fort_id, stop_location, stop.last_updated)
            if stop.last_updated and stop.last_updated > DatetimeWrapper.now() - timedelta_to_consider_deletion:
                logger.debug3("Stop considered for deletion was last updated recently, not gonna delete it for"
                              " now.", stop.last_updated)
                continue
            distance_to_location = get_distance_of_two_points_in_meters(float(stop_location.lat),
                                                                        float(stop_location.lng),
                                                                        float(self._worker_state.current_location.lat),
                                                                        float(self._worker_state.current_location.lng))
            logger.debug("Distance to {} at {} (last updated {})",
                         fort_id, stop_location, stop.last_updated)
            if distance_to_location < 100:
                logger.warning(
                    "Deleting stop {} at {} since it could not be found in the GMO but was present in DB and within "
                    "100m of worker ({}m) and was last updated more than 3 days ago ()",
                    fort_id, str(stop_location), distance_to_location, stop.last_updated)
                async with session.begin_nested() as nested_session:
                    await PokestopHelper.delete(session, stop_location)
                    try:
                        await nested_session.commit()
                    except exc.InternalError as e:
                        logger.warning("Failed deleting stop")
                        logger.exception(e)
                await self._mapping_manager.routemanager_add_coords_to_be_removed(self._area_id,
                                                                                  stop_location.lat,
                                                                                  stop_location.lng)

    async def _handle_stop(self, timestamp):
        self._stop_process_time = math.floor(timestamp)
        # Stop will automatically be spun, thus we only need to check for fort search protos of the stop(s) we are
        # waiting for.
        # TODO: Additionally, we may need to limit the amount of quests to be processed/accepted depending on the layer?
        to = 0
        timeout = 35
        type_received: ReceivedType = ReceivedType.UNDEFINED
        data_received = FortSearchResultTypes.UNDEFINED
        # TODO: Only try it once basically, then try clicking stop. Detect softban for sleeping?
        async with self._db_wrapper as session, session:
            while data_received != FortSearchResultTypes.QUEST and int(to) < 2:
                logger.info('Waiting for stop to be spun')
                type_received, data_received, time_received = await self._wait_for_data_after_moving(
                    self._stop_process_time, ProtoIdentifier.FORT_SEARCH, timeout)
                if (type_received == ReceivedType.FORT_SEARCH_RESULT
                        and data_received == FortSearchResultTypes.INVENTORY):
                    logger.warning('Box is full... clear out items!')
                    await asyncio.sleep(1)
                    await self._mapping_manager.routemanager_redo_stop_at_end(self._area_id,
                                                                              self._worker_state.origin,
                                                                              self._worker_state.current_location)
                    break
                elif (type_received == ReceivedType.FORT_SEARCH_RESULT
                      and (data_received == FortSearchResultTypes.QUEST
                           or data_received == FortSearchResultTypes.COOLDOWN
                           or (
                                   data_received == FortSearchResultTypes.FULL
                                   and await self._is_levelmode()))):
                    # Levelmode or data has been received...
                    if await self._is_levelmode():
                        logger.info("Saving visitation info...")
                        # This is leveling mode, it's faster to just ignore spin result and continue
                        break

                    if data_received == FortSearchResultTypes.COOLDOWN:
                        logger.info('Stop is on cooldown, moving on')
                        if await TrsQuestHelper.check_stop_has_quest(session, self._worker_state.current_location,
                                                                     self._quest_layer_to_scan):
                            logger.info('Quest is done without us noticing. Getting new Quest...')
                        else:
                            # We need to try again later on
                            await self._mapping_manager.routemanager_redo_stop_at_end(self._area_id,
                                                                                      self._worker_state.origin,
                                                                                      self._worker_state.current_location)
                        break
                    elif data_received == FortSearchResultTypes.QUEST:
                        logger.info('Received new Quest')

                elif (type_received == ReceivedType.FORT_SEARCH_RESULT
                      and (data_received == FortSearchResultTypes.TIME
                           or data_received == FortSearchResultTypes.OUT_OF_RANGE)):
                    logger.warning('Softban (type received: {}, data received: {}) - continuing......',
                                   type_received, data_received)
                    # TODO: Read last action and sleep for needed duration?
                elif (type_received == ReceivedType.FORT_SEARCH_RESULT
                      and data_received == FortSearchResultTypes.FULL):
                    logger.warning(
                        "Failed getting quest but got items - quest box is probably full. Starting cleanup "
                        "routine.")
                    break
                else:
                    logger.info("Failed retrieving stop spin...")
                    self._stop_process_time = math.floor(time.time())
                    if to > 2 and await TrsQuestHelper.check_stop_has_quest(session,
                                                                            self._worker_state.current_location,
                                                                            self._quest_layer_to_scan):
                        logger.info('Quest is done without us noticing. Getting new Quest...')
                        break
                    elif to > 2 and await self._is_levelmode() and await self._mitm_mapper.get_poke_stop_visits(
                            self._worker_state.origin) > 6800:
                        logger.warning("Might have hit a spin limit for worker! We have spun: {} stops",
                                       await self._mitm_mapper.get_poke_stop_visits(self._worker_state.origin))

                    await asyncio.sleep(1)
                to += 1
            else:
                if data_received != FortSearchResultTypes.QUEST:
                    # TODO
                    pass

    async def _get_unquest_stops(self) -> Set[str]:
        return await self._mapping_manager.routemanager_get_stops_with_quests(
            self._area_id)
