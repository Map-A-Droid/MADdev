import asyncio
import re
from ipaddress import IPv4Address, ip_address
from typing import Optional, Set

import websockets
from aiofile import async_open

from mapadroid.utils.collections import Location
from mapadroid.utils.CustomTypes import MessageTyping
from mapadroid.utils.geo import get_distance_of_two_points_in_meters
from mapadroid.utils.logging import LoggerEnums, get_logger
from mapadroid.utils.madGlobals import (
    MadGlobals, ScreenshotType, WebsocketWorkerConnectionClosedException,
    WebsocketWorkerTimeoutException)
from mapadroid.websocket.AbstractCommunicator import AbstractCommunicator
from mapadroid.websocket.WebsocketConnectedClientEntry import \
    WebsocketConnectedClientEntry
from mapadroid.worker.AbstractWorker import AbstractWorker

logger = get_logger(LoggerEnums.websocket)


class Communicator(AbstractCommunicator):
    def __init__(self, websocket_client_entry: WebsocketConnectedClientEntry, worker_id: str,
                 worker_instance_ref: Optional[AbstractWorker],
                 command_timeout: float):
        # Throws ValueError if unable to connect!
        # catch in code using this class
        self.worker_instance_ref: Optional[AbstractWorker] = worker_instance_ref
        self.websocket_client_entry = websocket_client_entry
        self.__command_timeout: float = command_timeout
        self.__send_mutex = asyncio.Lock()

    async def is_alive(self) -> bool:
        if not self.websocket_client_entry or not self.websocket_client_entry.websocket_client_connection:
            return False
        connection: websockets.WebSocketClientProtocol = self.websocket_client_entry.websocket_client_connection
        return connection.open

    async def cleanup(self) -> None:
        logger.info("Communicator calling exit to cleanup worker in websocket")
        try:
            await self.terminate_connection()
        except (WebsocketWorkerConnectionClosedException):
            logger.info("Communicator-cleanup: connection has already been closed")
        except WebsocketWorkerTimeoutException:
            logger.info("Timeout trying to close the connection gracefully. Force closing")
            try:
                await self.websocket_client_entry.websocket_client_connection.close()
            except Exception as e:
                logger.info("Failed closing connection forcefully: {}", e)

    async def __run_and_ok(self, command, timeout) -> bool:
        return await self.__run_and_ok_bytes(command, timeout)

    async def __run_get_gesponse(self, message: MessageTyping, timeout: float = None) -> Optional[MessageTyping]:
        async with self.__send_mutex:
            timeout = self.__command_timeout if timeout is None else timeout
            return await self.websocket_client_entry.send_and_wait(message, timeout=timeout,
                                                                   worker_instance=self.worker_instance_ref)

    async def __run_and_ok_bytes(self, message, timeout: float, byte_command: int = None) -> bool:
        async with self.__send_mutex:
            result = await self.websocket_client_entry.send_and_wait(message, timeout, self.worker_instance_ref,
                                                                     byte_command=byte_command)
            return result is not None and "OK" == result.strip()

    async def install_apk(self, timeout: float, filepath: str = None, data=None) -> bool:
        if not data:
            async with async_open(filepath, "rb") as file:  # opening for [r]eading as [b]inary
                data = await file.read()  # if you only wanted to read 512 bytes, do .read(512)
        return await self.__run_and_ok_bytes(message=data, timeout=timeout, byte_command=1)

    async def install_bundle(self, timeout: float, filepath: str = None, data=None) -> bool:
        if not data:
            async with async_open(filepath, "rb") as file:  # opening for [r]eading as [b]inary
                data = await file.read()  # if you only wanted to read 512 bytes, do .read(512)
        return await self.__run_and_ok_bytes(message=data, timeout=timeout, byte_command=2)

    async def start_app(self, package_name: str) -> bool:
        return await self.__run_and_ok("more start {}\r\n".format(package_name), self.__command_timeout)

    async def stop_app(self, package_name: str) -> bool:
        if not await self.__run_and_ok("more stop {}\r\n".format(package_name), self.__command_timeout):
            logger.error("Failed stopping {}, please check if SU has been granted", package_name)
            return False
        else:
            return True

    async def passthrough(self, command) -> Optional[MessageTyping]:
        return await self.websocket_client_entry.send_and_wait("passthrough {}".format(command),
                                                               self.__command_timeout,
                                                               self.worker_instance_ref)

    async def reboot(self) -> bool:
        return await self.__run_and_ok("more reboot now\r\n", self.__command_timeout)

    async def restart_app(self, package_name: str) -> bool:
        return await self.__run_and_ok("more restart {}\r\n".format(package_name), self.__command_timeout)

    async def reset_app_data(self, package_name: str) -> bool:
        return await self.__run_and_ok("more reset {}\r\n".format(package_name), self.__command_timeout)

    async def clear_app_cache(self, package_name: str) -> bool:
        return await self.__run_and_ok("more cache {}\r\n".format(package_name), self.__command_timeout)

    async def magisk_off(self) -> None:
        await self.passthrough("su -c magiskhide --disable")

    async def magisk_on(self) -> None:
        await self.passthrough("su -c magiskhide --enable")

    async def turn_screen_on(self) -> bool:
        return await self.__run_and_ok("more screen on\r\n", self.__command_timeout)

    async def click(self, click_x: int, click_y: int) -> bool:
        logger.debug('Click {} / {}', click_x, click_y)
        return await self.__run_and_ok(
            "screen click {} {}\r\n".format(str(int(round(click_x))), str(int(round(click_y)))),
            self.__command_timeout)

    async def swipe(self, x1: int, y1: int, x2: int, y2: int) -> Optional[MessageTyping]:
        return await self.__run_get_gesponse(
            "touch swipe {} {} {} {}\r\n".format(str(int(round(x1))), str(int(round(y1))),
                                                 str(int(round(x2))), str(int(round(y2)))))

    async def touch_and_hold(self, x1: int, y1: int, x2: int, y2: int, duration: int = 3000) -> bool:
        return await self.__run_and_ok("touch swipe {} {} {} {} {}".format(str(int(round(x1))), str(int(round(y1))),
                                                                           str(int(round(x2))), str(int(round(y2))),
                                                                           str(int(duration))), self.__command_timeout)

    async def get_screensize(self) -> Optional[MessageTyping]:
        return await self.__run_get_gesponse("screen size")

    async def get_y_offset(self) -> Optional[MessageTyping]:
        return await self.__run_get_gesponse("screen offset")

    async def uiautomator(self) -> Optional[MessageTyping]:
        return await self.__run_get_gesponse("more uiautomator")

    async def get_screenshot(self, path: str, quality: int = 70,
                             screenshot_type: ScreenshotType = ScreenshotType.JPEG) -> bool:
        if quality < 10 or quality > 100:
            logger.error("Invalid quality value passed for screenshots")
            return False

        screenshot_type_str: str = "jpeg"
        if screenshot_type == ScreenshotType.PNG:
            screenshot_type_str = "png"

        encoded = await self.__run_get_gesponse("screen capture {} {}\r\n".format(screenshot_type_str, quality))
        if encoded is None:
            return False
        elif isinstance(encoded, str):
            logger.debug2("Screenshot response not binary")
            if "KO: " in encoded:
                logger.error("get_screenshot: Could not retrieve screenshot. Make sure your RGC is updated.")
                return False
            elif "OK:" not in encoded:
                logger.error("get_screenshot: response not OK")
                return False
            return False
        else:
            logger.debug("Storing screenshot...")
            async with async_open(path, "wb") as fh:
                await fh.write(encoded)
            del encoded
            logger.debug2("Done storing, returning")
            return True

    async def back_button(self) -> bool:
        return await self.__run_and_ok("screen back\r\n", self.__command_timeout)

    async def home_button(self) -> bool:
        return await self.__run_and_ok("touch keyevent 3", self.__command_timeout)

    async def enter_text(self, text: str) -> bool:
        return await self.__run_and_ok("touch text " + str(text), self.__command_timeout)

    async def is_screen_on(self) -> bool:
        state = await self.__run_get_gesponse("more state screen\r\n")
        if state is None:
            return False
        return "on" in state

    async def is_pogo_topmost(self) -> bool:
        topmost = await self.__run_get_gesponse("more topmost app\r\n")
        if topmost is None:
            return False
        valid_pogo_states: Set[str] = {"com.nianticlabs.pokemongo",
                                       "ExternalAppBrowserActivity",  # PTC OAuth with Firefox
                                       "CustomTabActivity"  # PTC OAuth with Chrome
                                       }
        return any(x in topmost for x in valid_pogo_states)

    async def topmost_app(self) -> Optional[MessageTyping]:
        topmost = await self.__run_get_gesponse("more topmost app\r\n")
        if topmost and "KO:" in topmost:
            return None
        return topmost

    async def set_location(self, location: Location, altitude: float) -> Optional[MessageTyping]:
        return await self.__run_get_gesponse("geo fix {} {} {}\r\n".format(location.lat, location.lng, altitude))

    async def terminate_connection(self) -> bool:
        try:
            return await self.__run_and_ok("exit\r\n", timeout=5)
        except WebsocketWorkerConnectionClosedException:
            logger.info("Cannot gracefully terminate connection, it's already been closed")
            return True

    # coords need to be float values
    # speed integer with km/h
    #######
    # This blocks!
    #######
    async def walk_from_to(self, location_from: Location, location_to: Location, speed: float) -> Optional[
        MessageTyping]:
        # calculate the time it will take to walk and add it to the timeout!
        distance = get_distance_of_two_points_in_meters(
            location_from.lat, location_from.lng,
            location_to.lat, location_to.lng)
        # speed is in kmph, distance in m
        # we want m/s -> speed / 3.6
        speed_meters = speed / 3.6
        seconds_traveltime = distance / speed_meters
        return await self.__run_get_gesponse("geo walk {} {} {} {} {}\r\n".format(location_from.lat, location_from.lng,
                                                                                  location_to.lat, location_to.lng,
                                                                                  speed),
                                             self.__command_timeout + seconds_traveltime)

    # TODO: may require update for asyncio I/O
    async def get_compressed_logcat(self, path: str) -> bool:
        encoded = await self.__run_get_gesponse("more logcat\r\n")
        if encoded is None:
            return False
        elif isinstance(encoded, str):
            logger.debug("Logcat response not binary (expected a ZIP)")
            if "KO: " in encoded:
                logger.error(
                    "get_compressed_logcat: Could not retrieve logcat. Make sure your RGC is updated.")
            elif "OK:" not in encoded:
                logger.error("get_compressed_logcat: response not OK")
            return False
        else:
            logger.debug("Storing logcat...")

            async with async_open(path, "wb") as fh:
                await fh.write(encoded)
            logger.debug("Done storing logcat, returning")
            return True

    async def get_ptc_status(self) -> int:
        res: Optional[MessageTyping] = None
        try:
            res = await self.__run_get_gesponse(
                f"more http head https://sso.pokemon.com/sso/login\r\n")
            return int(res)
        except Exception as e:
            logger.warning("Failed retrieving SSO status of PTC: {} ({})", e, res)
            return 500

    async def get_external_ip(self) -> Optional[str]:
        try:
            res: Optional[MessageTyping] = await self.__run_get_gesponse(
                f"more http get {MadGlobals.application_args.ip_service}\r\n")
        except Exception as e:
            logger.error(f"Failed getting external IP address from device: {e}")
            return None
        if not res or res.startswith("Failed"):
            logger.error("Requesting IP failed ({})", res)
            return None
        # parse RGC return expression
        ip_address_found: Optional[str] = None
        try:
            # Regex ^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$ from
            # https://stackoverflow.com/questions/5284147/validating-ipv4-addresses-with-regexp
            found = re.search(r'(((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4})', res)
            if found:
                ip_address_found = found.group(1)
        except Exception as e:
            logger.error(f"Failed parsing external IP: {e}")
            return None

        if ip_address_found and type(ip_address(ip_address_found)) is IPv4Address:
            return ip_address_found
        else:
            logger.error(f"{ip_address_found} is not a valid IPv4 address")
            return None
