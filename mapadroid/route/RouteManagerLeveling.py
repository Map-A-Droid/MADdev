from typing import Dict, List, Optional, Tuple

from loguru import logger

from mapadroid.account_handler.AbstractAccountHandler import (
    AbstractAccountHandler, AccountPurpose)
from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.db.helper.PokestopHelper import PokestopHelper
from mapadroid.db.helper.SettingsDeviceHelper import SettingsDeviceHelper
from mapadroid.db.model import (Pokestop, SettingsAreaPokestop, SettingsDevice,
                                SettingsRoutecalc)
from mapadroid.geofence.geofenceHelper import GeofenceHelper
from mapadroid.route.routecalc.RoutecalcUtil import RoutecalcUtil
from mapadroid.route.RouteManagerBase import RouteManagerBase
from mapadroid.route.RoutePoolEntry import RoutePoolEntry
from mapadroid.utils.collections import Location
from mapadroid.utils.madGlobals import RoutecalculationTypes


class RouteManagerLeveling(RouteManagerBase):
    def purpose(self) -> AccountPurpose:
        return AccountPurpose.LEVEL

    async def _get_coords_fresh(self, dynamic: bool) -> List[Location]:
        # not necessary
        middle_of_fence: Tuple[float, float] = self.geofence_helper.get_middle_from_fence()
        return [Location(middle_of_fence[0], middle_of_fence[1])]

    def __init__(self, db_wrapper: DbWrapper, area: SettingsAreaPokestop, coords: Optional[List[Location]],
                 max_radius: int, max_coords_within_radius: int,
                 geofence_helper: GeofenceHelper, routecalc: SettingsRoutecalc,
                 account_handler: AbstractAccountHandler,
                 mon_ids_iv: Optional[List[int]] = None):
        RouteManagerBase.__init__(self, db_wrapper=db_wrapper, area=area, coords=coords,
                                  max_radius=max_radius, max_coords_within_radius=max_coords_within_radius,
                                  geofence_helper=geofence_helper, routecalc=routecalc,
                                  mon_ids_iv=mon_ids_iv,
                                  initial_prioq_strategy=None)
        self.remove_from_queue_backlog = None
        self.__account_handler = account_handler

    async def _worker_changed_update_routepools(self, routepool: Dict[str, RoutePoolEntry]) \
            -> Optional[Dict[str, RoutePoolEntry]]:
        async with self._manager_mutex:
            logger.info("Updating all routepools in level mode for {} origins", len(routepool))
            if len(self._workers_registered) == 0:
                logger.info("No registered workers, aborting __worker_changed_update_routepools...")
                return None

            any_at_all = False
            async with self.db_wrapper as session, session:
                for origin in routepool.keys():
                    origin_local_list = []
                    entry: Optional[RoutePoolEntry] = routepool.get(origin)
                    if not entry:
                        logger.debug("{} was removed during updating of routepools", origin)
                        continue
                    elif len(entry.queue) > 0:
                        logger.debug("origin {} already has a queue, do not touch...", origin)
                        continue
                    current_worker_pos = entry.current_pos

                    device: Optional[SettingsDevice] = await SettingsDeviceHelper.get_by_origin(session,
                                                                                                self.db_wrapper.get_instance_id(),
                                                                                                origin)

                    if not device:
                        logger.error("Device for origin {} not found", origin)
                        continue
                    username: Optional[str] = await self.__account_handler.get_assigned_username(device.device_id)
                    if not username:
                        logger.error("Unable to determine the username last assigned to {}", origin)
                        continue
                    unvisited_stops: List[Pokestop] = await PokestopHelper.get_nearby_increasing_range_within_area(
                        session,
                        geofence_helper=self.geofence_helper,
                        username=username,
                        location=current_worker_pos,
                        limit=150,
                        ignore_spun=True
                        if self._settings.ignore_spinned_stops or self._settings.ignore_spinned_stops is None
                        else False,
                        max_distance=1)
                    if not unvisited_stops:
                        logger.info("There are no unvisited stops left in DB for {} - nothing more to do!", origin)
                        continue

                    for stop in unvisited_stops:
                        coord_location = Location(float(stop.latitude), float(stop.longitude))
                        if coord_location in self._coords_to_be_ignored:
                            logger.info('Already tried this Stop but it failed spinnable test, skip it')
                            continue
                        origin_local_list.append(coord_location)

                    if unvisited_stops:
                        logger.info("Recalc a route")
                        new_route = await self._local_recalc_subroute(unvisited_stops)
                        origin_local_list.clear()
                        for coord in new_route:
                            origin_local_list.append(coord)

                    # subroute is all stops unvisited
                    logger.info("Origin {} has {} unvisited stops for this route", origin, len(origin_local_list))
                    entry.subroute = origin_local_list
                    # let's clean the queue just to make sure
                    entry.queue.clear()
                    [entry.queue.append(i) for i in origin_local_list]
                    any_at_all = len(origin_local_list) > 0 or any_at_all
                    # saving new startposition of walker in db
                    newstartposition: Location = entry.queue[0]
                    await SettingsDeviceHelper.save_last_walker_position(session,
                                                                         instance_id=self.db_wrapper.get_instance_id(),
                                                                         origin=origin,
                                                                         location=newstartposition)
                try:
                    await session.commit()
                except Exception as e:
                    logger.warning("Failed storing last walker positions: {}", e)
                return routepool

    async def _local_recalc_subroute(self, unvisited_stops: List[Pokestop]) -> List[Location]:
        coords: List[Location] = []
        for stop in unvisited_stops:
            coords.append(Location(float(stop.latitude), float(stop.longitude)))
        new_route: List[Location] = await RoutecalcUtil.calculate_route(self.db_wrapper,
                                                                        self._routecalc.routecalc_id,
                                                                        coords,
                                                                        self.get_max_radius(),
                                                                        self.get_max_coords_within_radius(),
                                                                        algorithm=RoutecalculationTypes.TSP_QUICK,
                                                                        use_s2=self.useS2,
                                                                        s2_level=self.S2level,
                                                                        route_name=self.name,
                                                                        overwrite_persisted_route=False,
                                                                        load_persisted_route=False)
        return new_route

    async def _any_coords_left_after_finishing_route(self) -> bool:
        # TODO: Return False/stop route of single worker based on whether that worker has any stops left...
        if self._shutdown_route.is_set():
            logger.info('Other worker shutdown route - leaving it')
            return False

        return await self._update_routepool()

    async def start_routemanager(self, skip_calculate: bool = False):
        async with self._manager_mutex:
            if not self._is_started.is_set():
                self._is_started.set()
                logger.info("Starting routemanager")

                if self._shutdown_route.is_set():
                    logger.info('Other worker shutdown route - leaving it')
                    return False

                self._prio_queue = None
                self.delay_after_timestamp_prio = None
                self.starve_route = False
                await self._start_check_routepools()

                return True
        return True

    async def _quit_route(self):
        logger.info('Shutdown Route')
        await super()._quit_route()

    def _check_coords_before_returning(self, lat: float, lng: float, origin):
        stop = Location(lat, lng)
        logger.info('Checking Stop with ID {}', stop)
        if stop in self._coords_to_be_ignored:
            logger.info('Already tried this Stop and failed it')
            return False
        logger.info('DB knows nothing of this stop for {} lets try and go there', origin)
        return True

    def _delete_coord_after_fetch(self) -> bool:
        return False

    def is_level_mode(self) -> bool:
        return True

    def get_quest_layer_to_scan(self) -> Optional[int]:
        return self._settings.layer
