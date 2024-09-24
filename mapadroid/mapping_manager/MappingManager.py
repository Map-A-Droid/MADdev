import asyncio
import copy
import time
from asyncio import Task
from datetime import datetime
from threading import Event
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from redis import WatchError
from redis import asyncio as aioredis
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from mapadroid.account_handler.AbstractAccountHandler import (
    AbstractAccountHandler, AccountPurpose)
from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.db.helper.SettingsAuthHelper import SettingsAuthHelper
from mapadroid.db.helper.SettingsDeviceHelper import SettingsDeviceHelper
from mapadroid.db.helper.SettingsDevicepoolHelper import \
    SettingsDevicepoolHelper
from mapadroid.db.helper.SettingsGeofenceHelper import SettingsGeofenceHelper
from mapadroid.db.helper.SettingsMonivlistHelper import SettingsMonivlistHelper
from mapadroid.db.helper.SettingsRoutecalcHelper import SettingsRoutecalcHelper
from mapadroid.db.helper.SettingsWalkerareaHelper import \
    SettingsWalkerareaHelper
from mapadroid.db.helper.SettingsWalkerHelper import SettingsWalkerHelper
from mapadroid.db.helper.SettingsWalkerToWalkerareaHelper import \
    SettingsWalkerToWalkerareaHelper
from mapadroid.db.model import (SettingsArea, SettingsAuth, SettingsDevice,
                                SettingsDevicepool, SettingsGeofence,
                                SettingsRoutecalc, SettingsWalker,
                                SettingsWalkerarea, SettingsWalkerToWalkerarea)
from mapadroid.geofence.geofenceHelper import GeofenceHelper
from mapadroid.mapping_manager.AbstractMappingManager import \
    AbstractMappingManager
from mapadroid.mapping_manager.MappingManagerDevicemappingKey import \
    MappingManagerDevicemappingKey
from mapadroid.route.prioq.strategy.AbstractRoutePriorityQueueStrategy import \
    RoutePriorityQueueEntry
from mapadroid.route.RouteManagerBase import RouteManagerBase
from mapadroid.route.RouteManagerFactory import RouteManagerFactory
from mapadroid.route.RouteManagerIV import RouteManagerIV
from mapadroid.utils.collections import Location
from mapadroid.utils.language import get_mon_ids
from mapadroid.utils.logging import LoggerEnums, get_logger
from mapadroid.utils.madGlobals import (MadGlobals, PositionType,
                                        RoutemanagerShuttingDown,
                                        ScreenshotType)
from mapadroid.worker.WorkerType import WorkerType

logger = get_logger(LoggerEnums.utils)

mode_mapping = {
    "raids_mitm": {
        "s2_cell_level": 15,
        "range": 490,
        "max_count": 100000
    },
    "mon_mitm": {
        "s2_cell_level": 17,
        "range": 67,
        "max_count": 100000
    },
    "pokestops": {
        "s2_cell_level": 13,
        "range": 67,
        "max_count": 3
    },
    "iv_mitm": {
        "range": 67,
        "max_count": 999999
    },
    "init": {
        "range": 67,
        "max_count": 1,
    }
}


class DeviceMappingsEntry:
    def __init__(self):
        self.device_settings: SettingsDevice = None
        self.pool_settings: SettingsDevicepool = None
        self.walker_areas: List[SettingsWalkerarea] = []
        # TODO: Ensure those values are being set properly from whereever...
        self.last_location: Location = Location(0.0, 0.0)
        self.last_known_mode: WorkerType = WorkerType.UNDEFINED
        self.account_index: int = 0
        self.account_rotation_started: bool = False
        self.walker_area_index: int = -1
        self.finished: bool = False
        self.job_active: bool = False
        self.last_location_time: Optional[int] = None
        self.last_cleanup_time: Optional[int] = None
        self.last_action_time: Optional[int] = None
        self.last_questclear_time: Optional[int] = None


class AreaEntry:
    def __init__(self):
        self.settings: SettingsArea = None
        self.routecalc: SettingsRoutecalc = None
        self.geofence_included: int = None
        self.geofence_excluded: int = None


class MappingManager(AbstractMappingManager):
    LOGIN_TRACKING_KEY_ORIGIN_IP_MAPPED: str = "login_tracking_ip_{}"
    LOGIN_TRACKING_TIMEOUT_ORIGIN_IP_MAPPED: int = 300

    def __init__(self, db_wrapper: DbWrapper, account_handler: AbstractAccountHandler, configmode: bool = False):
        self.__jobstatus: Dict = {}
        self.__db_wrapper: DbWrapper = db_wrapper
        self.__account_handler: AbstractAccountHandler = account_handler
        self.__configmode: bool = configmode

        self._devicemappings: Optional[Dict[str, DeviceMappingsEntry]] = None
        self._geofence_helpers: Optional[Dict[int, GeofenceHelper]] = None
        self._areas: Optional[Dict[int, AreaEntry]] = None
        self._routemanagers: Optional[Dict[int, RouteManagerBase]] = None
        self._auths: Optional[Dict[str, SettingsAuth]] = None
        self.__areamons: Optional[Dict[int, List[int]]] = {}
        self._monlists: Optional[Dict[int, List[int]]] = None
        self.__shutdown_event: Event = Event()

        # TODO: Move to init or call __init__ differently...
        self.__paused_devices: List[int] = []
        self.__mappings_mutex: Optional[asyncio.Lock] = None
        self.__ptc_mutex: Optional[asyncio.Lock] = None
        self._redis_cache: Optional[Redis] = None

    async def setup(self):
        self.__mappings_mutex: asyncio.Lock = asyncio.Lock()
        await self.setup_login_tracking()
        await self.update(full_lock=True)

    async def setup_login_tracking(self):
        if not MadGlobals.application_args.enable_login_tracking:
            return
        self.__ptc_mutex: asyncio.Lock = asyncio.Lock()
        # Check if special credentials have been passed, if not, just use the existing redis...
        if MadGlobals.application_args.login_tracking_host and MadGlobals.application_args.login_tracking_port:
            redis_credentials = {"host": MadGlobals.application_args.login_tracking_host,
                                 "port": MadGlobals.application_args.login_tracking_port}
            if MadGlobals.application_args.login_tracking_username:
                redis_credentials["username"] = MadGlobals.application_args.login_tracking_username
            if MadGlobals.application_args.login_tracking_password:
                redis_credentials["password"] = MadGlobals.application_args.login_tracking_password
            if MadGlobals.application_args.login_tracking_database:
                redis_credentials["db"] = MadGlobals.application_args.login_tracking_database
            self._redis_cache = await aioredis.Redis(**redis_credentials)
            await self._redis_cache.ping()
        else:
            self._redis_cache = await self.__db_wrapper.get_cache()

    def shutdown(self):
        logger.info("MappingManager exiting")

    async def get_auths(self) -> Optional[Dict[str, SettingsAuth]]:
        return self._auths

    def set_device_state(self, device_id: int, active: int) -> None:
        if active == 1:
            try:
                self.__paused_devices.remove(device_id)
            except ValueError:
                pass
        else:
            if device_id not in self.__paused_devices:
                self.__paused_devices.append(device_id)

    async def is_device_active(self, device_id: int) -> bool:
        return device_id not in self.__paused_devices

    async def get_paused_devices(self) -> Set[int]:
        return set(self.__paused_devices)

    async def get_devicemappings_of(self, device_name: str) -> Optional[DeviceMappingsEntry]:
        # Async method since we may move the logic to a different host
        return self._devicemappings.get(device_name, None)

    async def get_devicesettings_of(self, device_name: str) -> Optional[Tuple[SettingsDevice, SettingsDevicepool]]:
        devicemapping_entry: Optional[DeviceMappingsEntry] = self._devicemappings.get(device_name, None)
        if not devicemapping_entry:
            return None
        else:
            return devicemapping_entry.device_settings, devicemapping_entry.pool_settings

    async def get_levelmode(self, origin):
        device_routemananger = await self.get_routemanager_id_where_device_is_registered(origin)
        if device_routemananger is None:
            return False

        if await self.routemanager_is_levelmode(device_routemananger):
            return True

        return False

    async def get_safe_items(self, origin) -> List[int]:
        devicesettings: Optional[
            Tuple[SettingsDevice, SettingsDevicepool]] = await self.get_devicesettings_of(origin)
        if not devicesettings:
            return []
        values: str = ""
        if devicesettings[1] and devicesettings[1].enhanced_mode_quest_safe_items:
            values = devicesettings[1].enhanced_mode_quest_safe_items
        else:
            values = devicesettings[0].enhanced_mode_quest_safe_items
        if not values:
            values = "1401, 1402, 1403, 1106, 901, 902, 903, 501, 502, 503, 504, 301"
        return list(map(int, values.split(",")))

    async def __set_devicesetting(self, device_name: str, key: MappingManagerDevicemappingKey, value: Any) -> None:
        devicemapping_entry: Optional[DeviceMappingsEntry] = self._devicemappings.get(device_name, None)
        if not devicemapping_entry:
            return
        if key == MappingManagerDevicemappingKey.JOB_ACTIVE:
            devicemapping_entry.job_active = value
        elif key == MappingManagerDevicemappingKey.WALKER_AREA_INDEX:
            devicemapping_entry.walker_area_index = value
        elif key == MappingManagerDevicemappingKey.FINISHED:
            devicemapping_entry.finished = value
        elif key == MappingManagerDevicemappingKey.LAST_LOCATION_TIME:
            devicemapping_entry.last_location_time = value
        elif key == MappingManagerDevicemappingKey.LAST_CLEANUP_TIME:
            devicemapping_entry.last_cleanup_time = value
        elif key == MappingManagerDevicemappingKey.LAST_LOCATION:
            devicemapping_entry.last_location = value
        elif key == MappingManagerDevicemappingKey.ACCOUNT_INDEX:
            devicemapping_entry.account_index = value
        elif key == MappingManagerDevicemappingKey.LAST_MODE:
            devicemapping_entry.last_known_mode = value
        elif key == MappingManagerDevicemappingKey.LAST_ACTION_TIME:
            devicemapping_entry.last_action_time = value
        elif key == MappingManagerDevicemappingKey.LAST_QUESTCLEAR_TIME:
            devicemapping_entry.last_questclear_time = value
        elif key == MappingManagerDevicemappingKey.WALKER_AREA_INDEX:
            devicemapping_entry.walker_area_index = value
        else:
            # TODO: Maybe also set DB stuff? async with self.__db_wrapper as session, session:
            logger.warning("Unhandled key: {}", key)
            pass

    async def get_devicesetting_value_of_device(self, device_name: str, key: MappingManagerDevicemappingKey):
        devicemapping_entry: Optional[DeviceMappingsEntry] = self._devicemappings.get(device_name, None)
        if not devicemapping_entry:
            return
        if key == MappingManagerDevicemappingKey.JOB_ACTIVE:
            return devicemapping_entry.job_active
        elif key == MappingManagerDevicemappingKey.WALKER_AREA_INDEX:
            return devicemapping_entry.walker_area_index
        elif key == MappingManagerDevicemappingKey.FINISHED:
            return devicemapping_entry.finished
        elif key == MappingManagerDevicemappingKey.LAST_LOCATION_TIME:
            return devicemapping_entry.last_location_time
        elif key == MappingManagerDevicemappingKey.LAST_CLEANUP_TIME:
            return devicemapping_entry.last_cleanup_time
        elif key == MappingManagerDevicemappingKey.LAST_LOCATION:
            return devicemapping_entry.last_location
        elif key == MappingManagerDevicemappingKey.ACCOUNT_INDEX:
            return devicemapping_entry.account_index
        elif key == MappingManagerDevicemappingKey.LAST_MODE:
            return devicemapping_entry.last_known_mode
        elif key == MappingManagerDevicemappingKey.LAST_ACTION_TIME:
            return devicemapping_entry.last_action_time
        elif key == MappingManagerDevicemappingKey.LAST_QUESTCLEAR_TIME:
            return devicemapping_entry.last_questclear_time
        # DB stuff
        elif key == MappingManagerDevicemappingKey.SCREENSHOT_Y_OFFSET:
            return devicemapping_entry.pool_settings.screenshot_y_offset if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.screenshot_y_offset else devicemapping_entry.device_settings.screenshot_y_offset
        elif key == MappingManagerDevicemappingKey.SCREENSHOT_X_OFFSET:
            return devicemapping_entry.pool_settings.screenshot_x_offset if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.screenshot_x_offset else devicemapping_entry.device_settings.screenshot_x_offset
        elif key == MappingManagerDevicemappingKey.GGL_LOGIN_MAIL:
            return devicemapping_entry.device_settings.ggl_login_mail
        elif key == MappingManagerDevicemappingKey.STARTCOORDS_OF_WALKER:
            return devicemapping_entry.pool_settings.startcoords_of_walker if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.startcoords_of_walker else devicemapping_entry.device_settings.startcoords_of_walker
        elif key == MappingManagerDevicemappingKey.POST_TURN_SCREEN_ON_DELAY:
            return devicemapping_entry.pool_settings.post_turn_screen_on_delay if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.post_turn_screen_on_delay else devicemapping_entry.device_settings.post_turn_screen_on_delay
        elif key == MappingManagerDevicemappingKey.POST_SCREENSHOT_DELAY:
            return devicemapping_entry.pool_settings.post_screenshot_delay if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.post_screenshot_delay else devicemapping_entry.device_settings.post_screenshot_delay
        elif key == MappingManagerDevicemappingKey.POST_WALK_DELAY:
            return devicemapping_entry.pool_settings.post_walk_delay if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.post_walk_delay else devicemapping_entry.device_settings.post_walk_delay
        elif key == MappingManagerDevicemappingKey.POST_TELEPORT_DELAY:
            return devicemapping_entry.pool_settings.post_teleport_delay if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.post_teleport_delay else devicemapping_entry.device_settings.post_teleport_delay
        elif key == MappingManagerDevicemappingKey.WALK_AFTER_TELEPORT_DISTANCE:
            return devicemapping_entry.pool_settings.walk_after_teleport_distance if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.walk_after_teleport_distance else devicemapping_entry.device_settings.walk_after_teleport_distance
        elif key == MappingManagerDevicemappingKey.COOLDOWN_SLEEP:
            cool_down_sleep: bool = devicemapping_entry.pool_settings.cool_down_sleep if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.cool_down_sleep else devicemapping_entry.device_settings.cool_down_sleep
            return cool_down_sleep
        elif key == MappingManagerDevicemappingKey.POST_POGO_START_DELAY:
            return devicemapping_entry.pool_settings.post_pogo_start_delay if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.post_pogo_start_delay else devicemapping_entry.device_settings.post_pogo_start_delay
        elif key == MappingManagerDevicemappingKey.RESTART_POGO:
            return devicemapping_entry.pool_settings.restart_pogo if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.restart_pogo else devicemapping_entry.device_settings.restart_pogo
        elif key == MappingManagerDevicemappingKey.INVENTORY_CLEAR_ROUNDS:
            return devicemapping_entry.pool_settings.inventory_clear_rounds if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.inventory_clear_rounds else devicemapping_entry.device_settings.inventory_clear_rounds
        elif key == MappingManagerDevicemappingKey.MITM_WAIT_TIMEOUT:
            return devicemapping_entry.pool_settings.mitm_wait_timeout if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.mitm_wait_timeout else devicemapping_entry.device_settings.mitm_wait_timeout
        elif key == MappingManagerDevicemappingKey.VPS_DELAY:
            return devicemapping_entry.pool_settings.vps_delay if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.vps_delay else devicemapping_entry.device_settings.vps_delay
        elif key == MappingManagerDevicemappingKey.REBOOT:
            reboot: bool = devicemapping_entry.pool_settings.reboot if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.reboot else devicemapping_entry.device_settings.reboot
            return reboot
        elif key == MappingManagerDevicemappingKey.REBOOT_THRESH:
            return devicemapping_entry.pool_settings.reboot_thresh if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.reboot_thresh else devicemapping_entry.device_settings.reboot_thresh
        elif key == MappingManagerDevicemappingKey.RESTART_THRESH:
            return devicemapping_entry.pool_settings.restart_thresh if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.restart_thresh else devicemapping_entry.device_settings.restart_thresh
        elif key == MappingManagerDevicemappingKey.SCREENSHOT_TYPE:
            try:
                return ScreenshotType(
                    devicemapping_entry.pool_settings.screenshot_type if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.screenshot_type else devicemapping_entry.device_settings.screenshot_type)
            except ValueError:
                return ScreenshotType.JPEG
        elif key == MappingManagerDevicemappingKey.SCREENSHOT_QUALITY:
            quality: Optional[
                int] = devicemapping_entry.pool_settings.screenshot_quality if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.screenshot_quality else devicemapping_entry.device_settings.screenshot_quality
            return quality if quality else 80
        elif key == MappingManagerDevicemappingKey.INJECTION_THRESH_REBOOT:
            return devicemapping_entry.pool_settings.injection_thresh_reboot if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.injection_thresh_reboot else devicemapping_entry.device_settings.injection_thresh_reboot
        elif key == MappingManagerDevicemappingKey.SCREENDETECTION:
            screen_detection: Optional[
                bool] = devicemapping_entry.pool_settings.screendetection if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.screendetection else devicemapping_entry.device_settings.screendetection
            return screen_detection if screen_detection is not None else False
        elif key == MappingManagerDevicemappingKey.ENHANCED_MODE_QUEST_SAFE_ITEMS:
            return devicemapping_entry.pool_settings.enhanced_mode_quest_safe_items if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.enhanced_mode_quest_safe_items else devicemapping_entry.device_settings.enhanced_mode_quest_safe_items
        elif key == MappingManagerDevicemappingKey.CLEAR_GAME_DATA:
            return devicemapping_entry.device_settings.clear_game_data if devicemapping_entry.device_settings.clear_game_data is not None else False
        elif key == MappingManagerDevicemappingKey.SOFTBAR_ENABLED:
            return devicemapping_entry.device_settings.softbar_enabled if devicemapping_entry.device_settings.softbar_enabled is not None else False
        elif key == MappingManagerDevicemappingKey.ROTATION_WAITTIME:
            return devicemapping_entry.device_settings.rotation_waittime
        elif key == MappingManagerDevicemappingKey.ACCOUNT_ROTATION:
            return devicemapping_entry.device_settings.account_rotation
        elif key == MappingManagerDevicemappingKey.ROTATE_ON_LVL_30:
            return devicemapping_entry.device_settings.rotate_on_lvl_30
        elif key == MappingManagerDevicemappingKey.EXTENDED_PERMISSION_TOGGLING:
            return devicemapping_entry.device_settings.extended_permission_toggling
        elif key == MappingManagerDevicemappingKey.EXTENDED_LOGIN:
            return devicemapping_entry.pool_settings.extended_login if devicemapping_entry.pool_settings and devicemapping_entry.pool_settings.extended_login else devicemapping_entry.device_settings.extended_login
        else:
            # TODO: Get all the DB values...
            pass

    async def set_devicesetting_value_of(self, device_name: str, key: MappingManagerDevicemappingKey, value):
        if self._devicemappings.get(device_name, None) is not None:
            async with self.__mappings_mutex:
                await self.__set_devicesetting(device_name, key, value)

    async def get_all_devicemappings(self) -> Optional[Dict[str, DeviceMappingsEntry]]:
        return self._devicemappings

    async def get_all_loaded_origins(self) -> Set[str]:
        return set(self._devicemappings.keys()) if self._devicemappings else set()

    async def get_areas(self) -> Optional[Dict[int, AreaEntry]]:
        return self._areas

    def get_monlist(self, area_id) -> List[int]:
        try:
            return self.__areamons[area_id]
        except KeyError:
            return []

    async def get_all_routemanager_ids(self) -> List[int]:
        return list(self._routemanagers.keys())

    async def increment_login_tracking_by_origin(self, origin: str) -> bool:
        """
        Increments the login tracking counter for the IP stored mapped to the origin if there is one.
        """
        if not self.__ptc_mutex:
            return True
        async with self.__ptc_mutex:
            now = int(time.time())
            ip_retrieved: Optional[str] = await self._redis_cache.get(
                MappingManager.LOGIN_TRACKING_KEY_ORIGIN_IP_MAPPED.format(origin))
            if not ip_retrieved:
                logger.debug("Increment not needed as the IP was not stored for {}", origin)
                return False
            await self._redis_cache.delete(MappingManager.LOGIN_TRACKING_KEY_ORIGIN_IP_MAPPED.format(origin))
        logger.warning("Incrementing login tracking of {}", origin)
        async with self._redis_cache.pipeline() as pipe:
            pipe.multi()
            await pipe.zadd(ip_retrieved, {f"{origin}:{now}": now})
            await pipe.expire(ip_retrieved, 60 * 60 * 24)
            await pipe.execute()
        return True

    async def login_tracking_set_ip(self, origin: str, ip: str) -> None:
        if not self.__ptc_mutex:
            return
        await self._redis_cache.set(name=MappingManager.LOGIN_TRACKING_KEY_ORIGIN_IP_MAPPED.format(origin),
                                    value=ip,
                                    ex=MappingManager.LOGIN_TRACKING_TIMEOUT_ORIGIN_IP_MAPPED)

    async def login_tracking_remove_value(self, origin: str) -> None:
        if not self.__ptc_mutex:
            return
        async with self.__ptc_mutex:
            await self._redis_cache.delete(MappingManager.LOGIN_TRACKING_KEY_ORIGIN_IP_MAPPED.format(origin))

    async def ip_handle_login_request(self, ip, origin, limit_seconds=None, limit_count=None,
                                      increment_count: bool = True) -> bool:
        if not self.__ptc_mutex:
            return True
        if not limit_seconds:
            limit_seconds = int(MadGlobals.application_args.login_tracking_timeout)
        if not limit_count:
            limit_count = int(MadGlobals.application_args.login_tracking_limit)
        now = int(datetime.timestamp(datetime.now()))
        async with self.__ptc_mutex:
            logger.warning(f"Handle PTC login request on {ip}")
            async with self._redis_cache.pipeline() as pipe:
                while True:
                    try:
                        await pipe.watch(ip)
                        try:
                            ct = await self._redis_cache.zcount(ip, now - limit_seconds, "+inf")
                            # origin_logger.warning(f"got count {ct} for {ip} from redis")
                        except Exception as e:
                            logger.warning("Failed getting count for {} from redis! Deny this attempt ... "
                                           "({})", ip, e)
                            return False
                        if ct >= limit_count:
                            logger.warning("Reached threshold of {} attempts per {} "
                                           "seconds for {}! Deny this login attempt",
                                           limit_count, limit_seconds, ip)
                            return False
                        elif increment_count:
                            logger.info("Got count {} for {}. Allow this login attempt, register {}.",
                                        ct, ip, now)
                            pipe.multi()
                            await pipe.zadd(ip, {f"{origin}:{now}": now})
                            await pipe.expire(ip, 60 * 60 * 24)
                            await pipe.execute()
                            return True
                        else:
                            logger.info("Login tracking count is not to be incremented but no limit was found to apply.")
                            return True

                    except WatchError as e:
                        logger.warning(f"Redis count changed for {ip}, retry ... ({e})")
                        await asyncio.sleep(1)

    def __fetch_routemanager(self, routemanager_id: int) -> Optional[RouteManagerBase]:
        routemanager: RouteManagerBase = self._routemanagers.get(routemanager_id, None)
        if not routemanager:
            logger.debug2("No routemanager found with ID {}", routemanager_id)
        return routemanager

    async def routemanager_present(self, routemanager_id: int) -> bool:
        return routemanager_id in self._routemanagers.keys()

    async def routemanager_get_next_location(self, routemanager_id: int, origin: str) -> Optional[Location]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return await routemanager.get_next_location(origin) if routemanager is not None else None

    async def get_routemanager_id_where_device_is_registered(self, device: Union[str, int]) -> Optional[int]:
        """

        Args:
            device: either device_id (int) or origin/name (str)

        Returns:

        """
        origin: Optional[str] = None
        if isinstance(device, str):
            origin = device
        elif isinstance(device, int):
            for origin, entry in self._devicemappings.items():
                if entry.device_settings.device_id == device:
                    origin = origin
                    break

        routemanagers = await self.get_all_routemanager_ids()
        for routemanager in routemanagers:
            workers = await self.routemanager_get_registered_workers(routemanager)
            if origin in workers:
                return routemanager
        return None

    # def device_set_disabled(self, device_name: str) -> bool:
    #    routemanager = self.get_routemanager_id_where_device_is_registered(device_name)
    #    if routemanager is None:
    #        logger.info('Device {} is not registered so it cannot be paused', device_name)
    #        return False

    #        return True

    async def register_worker_to_routemanager(self, routemanager_id: int, worker_name: str) -> bool:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return await routemanager.register_worker(worker_name) if routemanager is not None else False

    async def unregister_worker_from_routemanager(self, routemanager_id: int, worker_name: str):
        routemanager = self.__fetch_routemanager(routemanager_id)
        return await routemanager.unregister_worker(worker_name) if routemanager is not None else None

    async def routemanager_add_coords_to_be_removed(self, routemanager_id: int, lat: float, lon: float):
        routemanager = self.__fetch_routemanager(routemanager_id)
        if routemanager is not None:
            routemanager.add_coord_to_be_removed(lat, lon)

    async def routemanager_get_stops_with_quests(self, routemanager_id: int) -> Set[str]:
        routemanager = self.__fetch_routemanager(routemanager_id=routemanager_id)
        return await routemanager.get_stop_ids_with_quests()

    async def routemanager_get_route_stats(self, routemanager_id: int, origin: str) -> Optional[Tuple[int, int]]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_route_status(origin) if routemanager is not None else None

    async def routemanager_get_rounds(self, routemanager_id: int) -> Optional[int]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_rounds() if routemanager is not None else None

    async def routemanager_redo_stop_immediately(self, routemanager_id: int, worker_name: str, lat: float,
                                                 lon: float) -> bool:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.redo_stop_immediately(worker_name, lat, lon) if routemanager is not None else False

    async def routemanager_get_registered_workers(self, routemanager_id: int) -> Set[str]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_registered_workers() if routemanager is not None else set()

    async def routemanager_get_ids_iv(self, routemanager_id: int) -> Optional[List[int]]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_ids_iv() if routemanager is not None else None

    async def routemanager_get_geofence_helper(self, routemanager_id: int) -> Optional[GeofenceHelper]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_geofence_helper() if routemanager is not None else None

    async def routemanager_is_levelmode(self, routemanager_id: int) -> bool:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.is_level_mode() if routemanager is not None else None

    async def routemanager_get_mode(self, routemanager_id: int) -> WorkerType:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_mode() if routemanager is not None else WorkerType.UNDEFINED.value

    async def routemanager_get_name(self, routemanager_id: int) -> Optional[str]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.name if routemanager is not None else None

    async def routemanager_get_encounter_ids_left(self, routemanager_id: int) -> Optional[List[int]]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        if routemanager is not None and isinstance(routemanager, RouteManagerIV):
            return routemanager.get_encounter_ids_left()
        else:
            return None

    async def routemanager_get_current_route(self, routemanager_id: int) -> Optional[Tuple[List[Location],
    Dict[
        str, List[Location]]]]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_current_route() if routemanager is not None else None

    async def routemanager_get_current_prioroute(self, routemanager_id: int) -> Optional[List[RoutePriorityQueueEntry]]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_current_prioroute() if routemanager is not None else None

    async def routemanager_get_settings(self, routemanager_id: int) -> Optional[SettingsArea]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_settings() if routemanager is not None else None

    async def routemanager_get_purpose_of_device(self, routemanager_id: int) -> Optional[AccountPurpose]:
        routemanager: Optional[RouteManagerBase] = self.__fetch_routemanager(routemanager_id)
        return None if not routemanager else routemanager.purpose()

    async def routemanager_set_worker_sleeping(self, routemanager_id: int, worker_name: str,
                                               sleep_duration: float):
        routemanager = self.__fetch_routemanager(routemanager_id)
        routemanager.set_worker_sleeping(worker_name, sleep_duration)

    async def routemanager_get_routecalc_id(self, routemanager_id: int) -> Optional[int]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_routecalc_id() if routemanager is not None else None

    async def set_worker_startposition(self, routemanager_id: int, worker_name: str,
                                       lat: float, lon: float):
        logger.debug("Fetching routemanager")
        routemanager = self.__fetch_routemanager(routemanager_id)
        logger.info("Setting routemanager's startposition")
        routemanager.set_worker_startposition(worker_name, lat, lon)

    async def routemanager_get_position_type(self, routemanager_id: int, worker_name: str) -> Optional[PositionType]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_position_type(worker_name) if routemanager is not None else None

    async def routemanager_get_max_radius(self, routemanager_id: int):
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_max_radius() if routemanager is not None else None

    async def routemanager_recalculate(self, routemanager_id: int) -> bool:
        """
        Recalculates the route with dynamic flag set to False
        :returns: Whether the recalculation was started or not
        """
        routemanager = self.__fetch_routemanager(routemanager_id)
        if not routemanager:
            logger.error("Routemanager {} is not loaded to recalculate a route.", routemanager_id)
            return False
        try:
            try:
                await routemanager.start_routemanager(skip_calculate=True)
            except RoutemanagerShuttingDown as e:
                logger.warning("Unable to start routemanager for recalc: {}", e)
                return False
            args = (False, True)
            kwargs = {
            }
            loop = asyncio.get_running_loop()
            loop.create_task(coro=routemanager.calculate_route(*args, **kwargs))
            return True
        except Exception:
            logger.opt(exception=True).error('Unable to start recalculation')
            return False

    async def __get_latest_geofence_helpers(self, session: AsyncSession) -> Dict[int, GeofenceHelper]:
        geofences: Dict[int, SettingsGeofence] = await SettingsGeofenceHelper.get_all_mapped(session,
                                                                                             self.__db_wrapper.get_instance_id())
        geofence_helpers: Dict[int, GeofenceHelper] = {}
        for geofence_id, geofence in geofences.items():
            geofence_helper = GeofenceHelper(geofence, None, geofence.name)
            geofence_helpers[geofence_id] = geofence_helper
        return geofence_helpers

    async def get_geofence_helper(self, geofence_id: int) -> Optional[GeofenceHelper]:
        return self._geofence_helpers.get(geofence_id)

    def __inherit_device_settings(self, devicesettings, poolsettings):
        inheritsettings = {}
        for pool_setting in poolsettings:
            inheritsettings[pool_setting] = poolsettings[pool_setting]
        for device_setting in devicesettings:
            inheritsettings[device_setting] = devicesettings[device_setting]
        return inheritsettings

    async def __get_latest_routemanagers(self, session: AsyncSession) -> Dict[int, RouteManagerBase]:
        # TODO: Use a factory for the iterations...
        global mode_mapping
        areas: Dict[int, SettingsArea] = {}

        if self.__configmode:
            return areas
        areas = await self.__db_wrapper.get_all_areas(session)
        # TODO: use amount of CPUs, use process pool?
        routemanagers: Dict[int, RouteManagerBase] = {}
        areas_procs: Dict[int, Task] = {}
        loop = asyncio.get_running_loop()
        for area_id, area in areas.items():
            if area.geofence_included is None:
                raise RuntimeError("Cannot work without geofence_included")

            try:
                geofence_included: Optional[SettingsGeofence] = await SettingsGeofenceHelper \
                    .get(session, self.__db_wrapper.get_instance_id(), area.geofence_included)
            except Exception:
                raise RuntimeError("geofence_included for area '{}' is specified but does not exist ('{}').".format(
                    area.name, area.geofence_included))

            geofence_excluded: Optional[SettingsGeofence] = None
            if area.mode in ("iv_mitm", "mon_mitm", 'pokestops', 'raids_mitm'):
                try:
                    if area.geofence_excluded is not None:
                        geofence_excluded = await SettingsGeofenceHelper \
                            .get(session, self.__db_wrapper.get_instance_id(), int(area.geofence_excluded))
                except Exception:
                    raise RuntimeError(
                        "geofence_excluded for area '{}' is specified but file does not exist ('{}').".format(
                            area.name, area.geofence_excluded
                        )
                    )
            # also build a routemanager for each area...

            # grab coords
            # first check if init is false, if so, grab the coords from DB
            geofence_helper = GeofenceHelper(geofence_included, geofence_excluded, area.name)
            # build routemanagers

            # TODO: Fill with all settings...
            area_settings: Dict[str, Any] = {}

            # map iv list to ids
            if area.mode in ("iv_mitm", "mon_mitm", "raids_mitm") and area.monlist_id:
                # replace list name
                area_settings['mon_ids_iv_raw'] = self.get_monlist(area_id)

            routecalc: Optional[SettingsRoutecalc] = await SettingsRoutecalcHelper \
                .get(session, area.routecalc)

            # TODO: Refactor most of the code in here moving it to the factory
            # TODO: Use use_s2 ?
            route_manager = RouteManagerFactory.get_routemanager(db_wrapper=self.__db_wrapper,
                                                                 area=area, coords=None,
                                                                 max_radius=mode_mapping.get(area.mode,
                                                                                             {}).get("range", 0),
                                                                 max_coords_within_radius=
                                                                 mode_mapping.get(area.mode, {}).get("max_count",
                                                                                                     99999999),
                                                                 geofence_helper=geofence_helper,
                                                                 routecalc=routecalc,
                                                                 s2_level=mode_mapping.get(area.mode, {}).get(
                                                                     "s2_cell_level", 30),
                                                                 mon_ids_iv=self.get_monlist(area_id),
                                                                 account_handler=self.__account_handler
                                                                 )
            logger.info("Initializing area {}", area.name)
            # task = loop.create_task(route_manager.calculate_route(False))
            # areas_procs[area_id] = task

            routemanagers[area.area_id] = route_manager
        for area in areas_procs.keys():
            # TODO: Async executors...
            to_be_checked: Task = areas_procs[area]
            try:
                await to_be_checked
            except RoutemanagerShuttingDown as e:
                logger.warning("Ignoring area {} ({}) due to failure to calculate route: {}", area,
                               routemanagers[area].name, e)
                del routemanagers[area]
        return routemanagers

    async def __get_latest_devicemappings(self, session: AsyncSession) -> Dict[str, DeviceMappingsEntry]:
        # returns mapping of devises to areas
        devices: Dict[str, DeviceMappingsEntry] = {}

        devices_of_instance: List[SettingsDevice] = await SettingsDeviceHelper \
            .get_all(session, self.__db_wrapper.get_instance_id())

        if not devices_of_instance:
            return devices

        all_walkers: Dict[int, SettingsWalker] = await SettingsWalkerHelper \
            .get_all_mapped(session, self.__db_wrapper.get_instance_id())
        all_walkerareas: Dict[int, SettingsWalkerarea] = await SettingsWalkerareaHelper \
            .get_all_mapped(session, self.__db_wrapper.get_instance_id())
        all_walkers_to_walkerareas: Dict[int, List[SettingsWalkerToWalkerarea]] = \
            await SettingsWalkerToWalkerareaHelper.get_all_mapped(session, self.__db_wrapper.get_instance_id())
        all_pools: Dict[int, SettingsDevicepool] = await SettingsDevicepoolHelper \
            .get_all_mapped(session, self.__db_wrapper.get_instance_id())

        for device in devices_of_instance:
            device_entry: DeviceMappingsEntry = DeviceMappingsEntry()
            device_entry.device_settings = device

            if device.pool_id is not None:
                device_entry.pool_settings = all_pools.get(device.pool_id, None)

            walker: SettingsWalker = all_walkers.get(device.walker_id, None)
            if walker:
                walkerarea_mappings_of_walker: List[SettingsWalkerToWalkerarea] = all_walkers_to_walkerareas \
                    .get(walker.walker_id, [])
                for walker_to_walkerareas in walkerarea_mappings_of_walker:
                    device_entry.walker_areas.append(all_walkerareas.get(walker_to_walkerareas.walkerarea_id))
            devices[device.name] = device_entry
        return devices

    async def __get_latest_auths(self, session: AsyncSession) -> Dict[str, SettingsAuth]:
        """
        Reads current self.__raw_json mappings dict and checks if auth directive is present.
        :return: Dict of username : password
        """
        all_auths: List[SettingsAuth] = await SettingsAuthHelper.get_all(session, self.__db_wrapper.get_instance_id())
        if all_auths is None or len(all_auths) == 0:
            return {}

        auths: Dict[str, SettingsAuth] = {}
        for auth in all_auths:
            auths[auth.username] = auth
        return auths

    async def __get_latest_areas(self, session: AsyncSession) -> Dict[int, AreaEntry]:
        areas: Dict[int, AreaEntry] = {}

        all_areas: Dict[int, SettingsArea] = await self.__db_wrapper.get_all_areas(session)

        if all_areas is None:
            return areas

        for area_id, area in all_areas.items():
            area_entry: AreaEntry = AreaEntry()
            area_entry.settings = area

            area_entry.routecalc = await SettingsRoutecalcHelper.get(session, area.routecalc)
            # getattr to avoid checking modes individually...
            area_entry.geofence_included = getattr(area, "geofence_included", None)
            area_entry.geofence_excluded = getattr(area, "geofence_excluded", None)

            areas[area_id] = area_entry
        return areas

    async def __get_latest_monlists(self, session: AsyncSession) -> Dict[int, List[int]]:
        return await SettingsMonivlistHelper.get_mapped_lists(session, self.__db_wrapper.get_instance_id())

    async def __get_latest_areamons(self, areas: Dict[int, AreaEntry]) -> Dict[int, List[int]]:
        """

        Args:
            areas:

        Returns: Dict with area ID (keys) and raw mon ID lists (values)

        """
        areamons: Dict[int, List[int]] = {}
        for area_id, area in areas.items():
            mon_iv_list_id: Optional[int] = getattr(area.settings, "monlist_id", None)
            all_mons: bool = getattr(area.settings, "all_mons", False)

            mon_list = []
            try:
                mon_list = copy.copy(self._monlists[int(mon_iv_list_id)])
            except (KeyError, TypeError):
                if not all_mons:
                    logger.warning(
                        "IV list '{}' has been used in area '{}' but does not exist. Using empty IV list"
                        "instead.", mon_iv_list_id, area.settings.name
                    )
                    areamons[area_id] = mon_list
                    continue
            except Exception as e:
                logger.exception(e)
            if all_mons:
                logger.debug("Area {} is configured for all mons", area.settings.name)
                for mon_id in await get_mon_ids():
                    if mon_id in mon_list:
                        continue
                    mon_list.append(int(mon_id))
            areamons[area_id] = mon_list
        return areamons

    async def update(self, full_lock=False) -> None:
        """
        Updates the internal mappings and routemanagers
        :return:
        """
        if not full_lock:
            async with self.__db_wrapper as session, session:
                self._monlists = await self.__get_latest_monlists(session)
                areas_tmp = await self.__get_latest_areas(session)
                self.__areamons = await self.__get_latest_areamons(areas_tmp)
                devicemappings_tmp: Dict[str, DeviceMappingsEntry] = await self.__get_latest_devicemappings(session)
                routemanagers_tmp = await self.__get_latest_routemanagers(session)
                geofence_helpers_tmp = await self.__get_latest_geofence_helpers(session)
                auths_tmp = await self.__get_latest_auths(session)

            for area_id, routemanager in self._routemanagers.items():
                logger.info("Stopping all routemanagers and join threads")
                await routemanager.stop_routemanager()
                await routemanager._stop_internal_tasks()

            logger.info("Restoring old devicesettings")
            for dev, mapping in self._devicemappings.items():
                if dev not in devicemappings_tmp:
                    logger.warning("{} is not in new devicemappings. "
                                   "This is fine in case of autoconfig running during a mappings update.")
                    continue
                devicemappings_tmp[dev].last_location = mapping.last_location
                devicemappings_tmp[dev].last_known_mode = mapping.last_known_mode
                devicemappings_tmp[dev].account_index = mapping.account_index
                devicemappings_tmp[dev].account_rotation_started = mapping.account_rotation_started
            logger.debug("Acquiring lock to update mappings")
            async with self.__mappings_mutex:
                self._areas = areas_tmp
                self._devicemappings = devicemappings_tmp
                self._routemanagers = routemanagers_tmp
                self._auths = auths_tmp
                self._geofence_helpers = geofence_helpers_tmp
            # Lastly, kill all strategies and update them accordingly

        else:
            logger.debug2("Acquiring lock to update mappings,full")
            async with self.__mappings_mutex:
                async with self.__db_wrapper as session, session:
                    self._areas = await self.__get_latest_areas(session)
                    self._monlists = await self.__get_latest_monlists(session)
                    self.__areamons = await self.__get_latest_areamons(self._areas)
                    self._routemanagers = await self.__get_latest_routemanagers(session)
                    self._devicemappings = await self.__get_latest_devicemappings(session)
                    self._auths = await self.__get_latest_auths(session)
                    self._geofence_helpers = await self.__get_latest_geofence_helpers(session)

        logger.info("Mappings have been updated")

    async def get_all_devicenames(self) -> List[str]:
        async with self.__db_wrapper as session, session:
            devices = []
            all_devices: List[SettingsDevice] = await SettingsDeviceHelper.get_all(session,
                                                                                   self.__db_wrapper.get_instance_id())
            for device in all_devices:
                devices.append(device.name)
            devices.sort()
            return devices

    def get_jobstatus(self) -> Dict:
        return self.__jobstatus

    async def routemanager_of_origin_is_levelmode(self, origin: str) -> bool:
        if not origin:
            # just avoid undefined behaviour....
            return False
        device_routemananger: Optional[int] = await self.get_routemanager_id_where_device_is_registered(origin)
        if not device_routemananger:
            return False
        return await self.routemanager_is_levelmode(device_routemananger)

    async def routemanager_get_quest_layer_to_scan_of_origin(self, origin: str) -> Optional[int]:
        if not origin:
            # just avoid undefined behaviour....
            return None
        device_routemananger: Optional[int] = await self.get_routemanager_id_where_device_is_registered(origin)
        if not device_routemananger:
            return None
        return await self.routemanager_get_quest_layer_to_scan(device_routemananger)

    async def routemanager_get_quest_layer_to_scan(self, routemanager_id: int) -> Optional[int]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return routemanager.get_quest_layer_to_scan() if routemanager is not None else None

    async def routemanager_redo_stop_at_end(self, routemanager_id: int, worker_name: str, location: Location) -> None:
        routemanager = self.__fetch_routemanager(routemanager_id)
        routemanager.redo_stop_at_end(worker_name, location)

    async def routemanager_get_amount_of_coords_scannable(self, routemanager_id: int) -> Optional[int]:
        routemanager = self.__fetch_routemanager(routemanager_id)
        return await routemanager.get_amount_of_coords_scannable() if routemanager is not None else None
