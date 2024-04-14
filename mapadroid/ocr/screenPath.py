import asyncio
import os
import random
import re
import time
import xml.etree.ElementTree as ET  # noqa: N817
from enum import Enum
from typing import List, Optional, Tuple

from loguru import logger

from mapadroid.account_handler import fetch_auth_details
from mapadroid.account_handler.AbstractAccountHandler import (
    AbstractAccountHandler, BurnType)
from mapadroid.db.model import SettingsPogoauth
from mapadroid.geofence.geofenceHelper import GeofenceHelper
from mapadroid.mapping_manager import MappingManager
from mapadroid.mapping_manager.MappingManagerDevicemappingKey import \
    MappingManagerDevicemappingKey
from mapadroid.ocr.screen_type import ScreenType
from mapadroid.utils.collections import Location, ScreenCoordinates
from mapadroid.utils.CustomTypes import MessageTyping
from mapadroid.utils.madGlobals import MadGlobals, ScreenshotType
from mapadroid.websocket.AbstractCommunicator import AbstractCommunicator
from mapadroid.worker.WorkerState import WorkerState


class LoginType(Enum):
    UNKNOWN = -1
    google = 1
    ptc = 2


class WordToScreenMatching(object):
    def __init__(self, communicator: AbstractCommunicator, worker_state: WorkerState,
                 mapping_mananger: MappingManager, account_handler: AbstractAccountHandler):
        # TODO: Somehow prevent call from elsewhere? Raise exception and only init in WordToScreenMatching.create?
        self._worker_state: WorkerState = worker_state
        self._mapping_manager: MappingManager = mapping_mananger
        self._account_handler: AbstractAccountHandler = account_handler
        self._nextscreen: ScreenType = ScreenType.UNDEFINED

        self._communicator: AbstractCommunicator = communicator
        logger.info("Starting Screendetector")

    @classmethod
    async def create(cls, communicator: AbstractCommunicator, worker_state: WorkerState,
                     mapping_mananger: MappingManager, account_handler: AbstractAccountHandler):
        self = WordToScreenMatching(communicator=communicator, worker_state=worker_state,
                                    mapping_mananger=mapping_mananger,
                                    account_handler=account_handler)
        self._accountindex = await self.get_devicesettings_value(MappingManagerDevicemappingKey.ACCOUNT_INDEX, 0)
        return self

    async def __evaluate_topmost_app(self, topmost_app: str) -> Tuple[ScreenType, dict, int]:
        returntype: ScreenType = ScreenType.UNDEFINED
        global_dict: dict = {}
        diff = 1
        if "ExternalAppBrowserActivity" in topmost_app or "CustomTabActivity" in topmost_app:
            return ScreenType.PTC, global_dict, diff
        if "AccountPickerActivity" in topmost_app or 'SignInActivity' in topmost_app:
            return ScreenType.GGL, global_dict, diff
        elif "GrantPermissionsActivity" in topmost_app:
            return ScreenType.PERMISSION, global_dict, diff
        elif "GrantCredentialsWithAclNoTouchActivity" in topmost_app or "GrantCredentials" in topmost_app:
            return ScreenType.CREDENTIALS, global_dict, diff
        elif "ConsentActivity" in topmost_app:
            return ScreenType.CONSENT, global_dict, diff
        elif "/a.m" in topmost_app:
            logger.error("Likely found 'not responding' popup - reboot device (topmost app: {})", topmost_app)
            return ScreenType.NOTRESPONDING, global_dict, diff
        elif "com.nianticlabs.pokemongo" not in topmost_app:
            logger.warning("PoGo is not opened! Current topmost app: {}", topmost_app)
            return ScreenType.CLOSE, global_dict, diff
        elif self._nextscreen != ScreenType.UNDEFINED:
            # TODO: how can the nextscreen be known in the current? o.O
            return self._nextscreen, global_dict, diff
        elif not await self.get_devicesettings_value(MappingManagerDevicemappingKey.SCREENDETECTION, True):
            logger.info('Screen detection is disabled')
            return ScreenType.DISABLED, global_dict, diff
        else:
            result = await self._take_and_analyze_screenshot()
            if not result:
                logger.error("_check_windows: Failed getting/analyzing screenshot")
                return ScreenType.ERROR, global_dict, diff
            else:
                returntype, global_dict, diff = result
            if not global_dict:
                self._nextscreen = ScreenType.UNDEFINED
                logger.warning('Could not understand any text on screen - starting next round...')
                return ScreenType.ERROR, global_dict, diff

            logger.debug("Screenratio: {}", self._worker_state.resolution_calculator.x_y_ratio)

            if 'text' not in global_dict:
                logger.error('Error while text detection')
                return ScreenType.ERROR, global_dict, diff
            elif returntype == ScreenType.UNDEFINED and "com.nianticlabs.pokemongo" in topmost_app:
                return ScreenType.POGO, global_dict, diff

        return returntype, global_dict, diff

    async def __handle_login_screen(self, global_dict: dict, diff: int) -> None:
        temp_dict: dict = {}
        n_boxes = len(global_dict['text'])
        logger.debug("Selecting login with: {}", global_dict)
        if not self._worker_state.active_account:
            logger.error("No account set for device, sleeping 30s")
            await asyncio.sleep(30)
            return

        for i in range(n_boxes):
            if 'Facebook' in (global_dict['text'][i]):
                temp_dict['Facebook'] = global_dict['top'][i] / diff
            if 'CLUB' in (global_dict['text'][i]):
                temp_dict['CLUB'] = global_dict['top'][i] / diff
            # french ...
            if 'DRESSEURS' in (global_dict['text'][i]):
                temp_dict['CLUB'] = global_dict['top'][i] / diff
            if 'Google' in (global_dict['text'][i]):
                temp_dict['Google'] = global_dict['top'][i] / diff

            if self._worker_state.active_account \
                    and self._worker_state.active_account.login_type == LoginType.ptc.name:
                self._nextscreen = ScreenType.PTC
                if 'CLUB' in (global_dict['text'][i]):
                    logger.info("ScreenType.LOGINSELECT (c) using PTC (logintype in Device Settings)")
                    await self._click_center_button(diff, global_dict, i)
                    await asyncio.sleep(5)
                    return

                # alternative select - calculate down from Facebook button
                elif 'Facebook' in temp_dict:
                    click_x = self._worker_state.resolution_calculator.screen_size_x / 2
                    click_y = (temp_dict[
                                   'Facebook'] + 2 * self._worker_state.resolution_calculator.screen_size_y / 10.11)
                    logger.info("ScreenType.LOGINSELECT (f) using PTC (logintype in Device Settings)")
                    await self._communicator.click(int(click_x), int(click_y))
                    await asyncio.sleep(5)
                    return

                # alternative select - calculate down from Google button
                elif 'Google' in temp_dict:
                    click_x = self._worker_state.resolution_calculator.screen_size_x / 2
                    click_y = (temp_dict['Google'] + self._worker_state.resolution_calculator.screen_size_y / 10.11)
                    logger.info("ScreenType.LOGINSELECT (g) using PTC (logintype in Device Settings)")
                    await self._communicator.click(int(click_x), int(click_y))
                    await asyncio.sleep(5)
                    return

            else:
                self._nextscreen = ScreenType.UNDEFINED
                if 'Google' in (global_dict['text'][i]):
                    logger.info("ScreenType.LOGINSELECT (g) using Google Account (logintype in Device Settings)")
                    await self._click_center_button(diff, global_dict, i)
                    await asyncio.sleep(5)
                    return

                # alternative select
                elif 'Facebook' in temp_dict and 'CLUB' in temp_dict:
                    click_x = self._worker_state.resolution_calculator.screen_size_x / 2
                    click_y = (temp_dict['Facebook'] + ((temp_dict['CLUB'] - temp_dict['Facebook']) / 2))
                    logger.info("ScreenType.LOGINSELECT (fc) using Google Account (logintype in Device Settings)")
                    await self._communicator.click(int(click_x), int(click_y))
                    await asyncio.sleep(5)
                    return

                # alternative select
                elif 'Facebook' in temp_dict:
                    click_x = self._worker_state.resolution_calculator.screen_size_x / 2
                    click_y = (temp_dict['Facebook'] + self._worker_state.resolution_calculator.screen_size_y / 10.11)
                    logger.info("ScreenType.LOGINSELECT (f) using Google Account (logintype in Device Settings)")
                    await self._communicator.click(int(click_x), int(click_y))
                    await asyncio.sleep(5)
                    return

                # alternative select
                elif 'CLUB' in temp_dict:
                    click_x = self._worker_state.resolution_calculator.screen_size_x / 2
                    click_y = (temp_dict['CLUB'] - self._worker_state.resolution_calculator.screen_size_y / 10.11)
                    logger.info("ScreenType.LOGINSELECT (c) using Google Account (logintype in Device Settings)")
                    await self._communicator.click(int(click_x), int(click_y))
                    await asyncio.sleep(5)
                    return

    async def check_ptc_login_ban(self, increment_count: bool = True) -> bool:
        """
        Checks whether a PTC login is currently permissible.
        :return: True, if PTC login can be run through. False, otherwise.
        """
        logger.debug("Checking for PTC login permission")
        ip = await self._communicator.get_external_ip()
        if not ip:
            logger.warning("Unable to get IP from device. Deny PTC login request")
            return False
        code = await self._communicator.get_ptc_status() or 500
        if code == 200:
            logger.debug("OK - PTC returned {} on {}", code, ip)
            return await self._mapping_manager.ip_handle_login_request(ip, self._worker_state.origin,
                                                                       increment_count=increment_count)
        elif code == 403:
            logger.warning("PTC ban is active ({}) on {}", code, ip)
            return False
        else:
            logger.info("PTC login server returned {} on {} - do not log in!", code, ip)
            return False

    async def _click_center_button(self, diff, global_dict, i) -> None:
        (x, y, w, h) = (global_dict['left'][i], global_dict['top'][i],
                        global_dict['width'][i], global_dict['height'][i])
        logger.debug("Diff: {}", diff)
        click_x, click_y = (x + w / 2) / diff, (y + h / 2) / diff
        await self._communicator.click(click_x, click_y)

    async def __handle_screentype(self, screentype: ScreenType,
                                  global_dict: Optional[dict] = None, diff: int = -1,
                                  y_offset: int = 0) -> ScreenType:
        if screentype == ScreenType.UNDEFINED:
            logger.warning("Undefined screentype, abandon ship...")
        elif screentype == ScreenType.BIRTHDATE:
            await fetch_auth_details(mapping_manager=self._mapping_manager,
                                     worker_state=self._worker_state,
                                     account_handler=self._account_handler)
            await self.__handle_birthday_screen()
        elif screentype == ScreenType.RETURNING:
            await self.__handle_returning_player_or_wrong_credentials()
        elif screentype == ScreenType.LOGINSELECT:
            await fetch_auth_details(mapping_manager=self._mapping_manager,
                                     worker_state=self._worker_state,
                                     account_handler=self._account_handler)
            await self.__handle_login_screen(global_dict, diff)
        elif screentype == ScreenType.PTC:
            await fetch_auth_details(mapping_manager=self._mapping_manager,
                                     worker_state=self._worker_state,
                                     account_handler=self._account_handler)
            return await self.__handle_ptc_login()
        elif screentype == ScreenType.FAILURE:
            await self.__handle_failure_screen()
        elif screentype == ScreenType.RETRY:
            if MadGlobals.application_args.enable_early_maintenance_detection and self._worker_state.maintenance_early_detection_triggered:
                logger.warning("Seen RETRY screen after multiple proto timeouts - most likely MAINTENANCE")
                await self._account_handler.mark_burnt(self._worker_state.device_id, BurnType.MAINTENANCE)
            await self.__handle_retry_screen(diff, global_dict)
        elif screentype == ScreenType.WRONG:
            await self.__handle_returning_player_or_wrong_credentials()
            screentype = ScreenType.ERROR
        elif screentype == ScreenType.LOGINTIMEOUT:
            await self.__handle_login_timeout(diff, global_dict)
        elif screentype == ScreenType.GAMEDATA:
            self._nextscreen = ScreenType.UNDEFINED
        elif screentype == ScreenType.GGL:
            screentype = await self.__handle_google_login(screentype)
        elif screentype == ScreenType.PERMISSION:
            screentype = await self.__handle_permissions_screen(screentype)
        elif screentype == ScreenType.CREDENTIALS:
            screentype = await self.__handle_permissions_screen(screentype)
        elif screentype == ScreenType.MARKETING:
            await self.__handle_marketing_screen(diff, global_dict)
        elif screentype == ScreenType.CONSENT:
            screentype = await self.__handle_ggl_consent_screen()
        elif screentype == ScreenType.WELCOME:
            screentype = await self.__handle_welcome_screen()
        elif screentype == ScreenType.TOS:
            screentype = await self.__handle_tos_screen()
        elif screentype == ScreenType.PRIVACY:
            screentype = await self.__handle_privacy_screen()
        elif screentype == ScreenType.WILLOWCHAR:
            screentype = await self.__handle_character_selection_screen()
        elif screentype == ScreenType.WILLOWCATCH:
            screentype = await self.__handle_catch_tutorial()
        elif screentype == ScreenType.WILLOWNAME:
            screentype = await self.__handle_name_screen()
        elif screentype == ScreenType.ADVENTURESYNC:
            screentype = await self.__handle_adventure_sync_screen(screentype)
        elif screentype == ScreenType.WILLOWGO:
            screentype = await self.__handle_tutorial_end()
        elif screentype == ScreenType.SN:
            self._nextscreen = ScreenType.UNDEFINED
        elif screentype == ScreenType.UPDATE:
            self._nextscreen = ScreenType.UNDEFINED
        elif screentype == ScreenType.NOGGL:
            self._nextscreen = ScreenType.UNDEFINED
        elif screentype == ScreenType.STRIKE:
            await self.__handle_strike_screen(diff, global_dict)
        elif screentype == ScreenType.SUSPENDED:
            self._nextscreen = ScreenType.UNDEFINED
            logger.warning('Account temporarily banned!')
            await self._account_handler.mark_burnt(self._worker_state.device_id,
                                                   BurnType.SUSPENDED)
            screentype = ScreenType.ERROR
        elif screentype == ScreenType.TERMINATED:
            self._nextscreen = ScreenType.UNDEFINED
            logger.error('Account permabanned!')
            await self._account_handler.mark_burnt(self._worker_state.device_id,
                                                   BurnType.BAN)
            screentype = ScreenType.ERROR
        elif screentype == ScreenType.MAINTENANCE:
            self._nextscreen = ScreenType.UNDEFINED
            logger.warning('Account saw maintenance warning!')
            await self._account_handler.mark_burnt(self._worker_state.device_id,
                                                   BurnType.MAINTENANCE)
        elif screentype == ScreenType.LIMITATIONS:
            self._nextscreen = ScreenType.UNDEFINED
            logger.warning('Account saw limitations/maintenance warning!')
            await self._account_handler.mark_burnt(self._worker_state.device_id,
                                                   BurnType.MAINTENANCE)
        elif screentype == ScreenType.POGO:
            screentype = await self.__check_pogo_screen_ban_or_loading(screentype, y_offset=y_offset)
        elif screentype == ScreenType.QUEST:
            logger.warning("Already on quest screen")
            # TODO: consider closing quest window?
            self._nextscreen = ScreenType.UNDEFINED
        elif screentype == ScreenType.GPS:
            self._nextscreen = ScreenType.UNDEFINED
            logger.warning("In game error detected")
        elif screentype == ScreenType.BLACK:
            logger.warning("Screen is black, sleeping a couple seconds for another check...")
        elif screentype == ScreenType.CLOSE:
            logger.debug("Detected pogo not open")
        elif screentype == ScreenType.DISABLED:
            logger.warning("Screendetection disabled")
        elif screentype == ScreenType.ERROR:
            logger.error("Error during screentype detection")

        return screentype

    async def __check_pogo_screen_ban_or_loading(self, screentype, y_offset: int = 0) -> ScreenType:
        screenshot_path: str = await self.get_screenshot_path()
        backgroundcolor = await self._worker_state.pogo_windows.most_frequent_colour(screenshot_path,
                                                                                     self._worker_state.origin,
                                                                                     y_offset=y_offset)
        globaldict = await self._worker_state.pogo_windows.get_screen_text(screenshot_path, self._worker_state.origin)
        welcome_text = ['Willkommen', 'Welcome']
        if backgroundcolor is not None and (
                backgroundcolor[0] == 0 and
                backgroundcolor[1] == 0 and
                backgroundcolor[2] == 0):
            # Background is black - Loading ...
            screentype = ScreenType.BLACK
        elif backgroundcolor is not None and (
                backgroundcolor[0] == 16 and
                backgroundcolor[1] == 24 and
                backgroundcolor[2] == 33):
            # Got a strike warning
            screentype = ScreenType.STRIKE
        elif backgroundcolor is not None and (
                any(text in welcome_text for text in globaldict['text']) or (
                backgroundcolor[0] == 18 and
                backgroundcolor[1] == 46 and
                backgroundcolor[2] == 86) or (
                        backgroundcolor[0] == 55 and
                        backgroundcolor[1] == 72 and
                        backgroundcolor[2] == 88)):
            screentype = ScreenType.WELCOME
        return screentype

    async def __handle_strike_screen(self, diff, global_dict) -> None:
        self._nextscreen = ScreenType.UNDEFINED
        logger.warning('Got a black strike warning!')
        click_text = 'GOT IT,ALLES KLAR'
        n_boxes = len(global_dict['text'])
        for i in range(n_boxes):
            if any(elem.lower() in (global_dict['text'][i].lower()) for elem in click_text.split(",")):
                await self._click_center_button(diff, global_dict, i)
                await asyncio.sleep(2)

    async def __handle_marketing_screen(self, diff, global_dict) -> None:
        self._nextscreen = ScreenType.POGO
        click_text = 'ERLAUBEN,ALLOW,AUTORISER'
        n_boxes = len(global_dict['text'])
        for i in range(n_boxes):
            if any(elem.lower() in (global_dict['text'][i].lower()) for elem in click_text.split(",")):
                await self._click_center_button(diff, global_dict, i)
                await asyncio.sleep(2)

    async def __handle_permissions_screen(self, screentype) -> ScreenType:
        self._nextscreen = ScreenType.UNDEFINED
        if not await self.parse_permission(await self._communicator.uiautomator()):
            screentype = ScreenType.ERROR
        await asyncio.sleep(4)
        return screentype

    async def __handle_google_login(self, screentype) -> ScreenType:
        self._nextscreen = ScreenType.UNDEFINED
        usernames: Optional[str] = None
        if not self._worker_state.active_account:
            logger.error("No account set for device, sleeping 30s")
            await asyncio.sleep(30)
            return ScreenType.ERROR
        elif self._worker_state.active_account and self._worker_state.active_account.login_type == LoginType.ptc.name:
            logger.warning("Google login was opened but PTC login is expected, restarting pogo")
            await self._communicator.restart_app("com.nianticlabs.pokemongo")
            await asyncio.sleep(50)
            return ScreenType.PTC
        elif self._worker_state.active_account:
            usernames: Optional[str] = self._worker_state.active_account.username
        else:
            logger.error("No active account set in worker_state")
        if not usernames:
            logger.error("Failed determining which google account to use")
            return ScreenType.ERROR
        usernames_to_check_for: List[str] = usernames.split(",")
        if await self.parse_ggl(await self._communicator.uiautomator(), usernames_to_check_for):
            logger.info("Sleeping 120 seconds after clicking the account to login with - please wait!")
            await asyncio.sleep(120)
            if await self.get_devicesettings_value(MappingManagerDevicemappingKey.EXTENDED_LOGIN, False):
                logger.info("Extended login enabled. Restarting pogo with PD fully enabled again")
                await self._communicator.passthrough(
                    "su -c 'am broadcast -a com.mad.pogodroid.SET_INTENTIONAL_STOP -c android.intent.category.DEFAULT -n com.mad.pogodroid/.IntentionalStopSetterReceiver --ez value false'")
                await asyncio.sleep(2)
                await self._communicator.passthrough(
                    "su -c 'am start-foreground-service -n com.mad.pogodroid/.services.HookReceiverService'")
                await asyncio.sleep(5)
                await self._communicator.stop_app("com.nianticlabs.pokemongo")
                await asyncio.sleep(10)
                await self._communicator.start_app("com.nianticlabs.pokemongo")
                await asyncio.sleep(120)
        else:
            screentype = ScreenType.ERROR
        return screentype

    async def __handle_retry_screen(self, diff, global_dict) -> None:
        self._nextscreen = ScreenType.UNDEFINED
        # forcing clear_game_data here due to Niantic changes and game now remembering/pre-filling username on login
        await self.clear_game_data()
        # after clear_game_data there should be no reason to click this button as game gets killed
        # but let's leave it here if Niantic decides this is a bug rather than QOL change
        # click_text = 'DIFFERENT,AUTRE,AUTORISER,ANDERES,KONTO,ACCOUNT'
        # await self.__click_center_button_text(click_text, diff, global_dict)

    async def __click_center_button_text(self, click_text, diff, global_dict):
        n_boxes = len(global_dict['text'])
        for i in range(n_boxes):
            if any(elem in (global_dict['text'][i]) for elem in click_text.split(",")):
                await self._click_center_button(diff, global_dict, i)
                await asyncio.sleep(2)

    async def __handle_ptc_login(self) -> ScreenType:
        self._nextscreen = ScreenType.UNDEFINED
        if not self._worker_state.active_account:
            logger.error('No PTC Username and Password is set')
            return ScreenType.ERROR
        elif self._worker_state.active_account.login_type == LoginType.ptc.google:
            logger.warning("PTC login was opened but google login is expected, restarting pogo")
            await self._communicator.restart_app("com.nianticlabs.pokemongo")
            await asyncio.sleep(50)
            return ScreenType.GGL
        xml: Optional[MessageTyping] = await self._communicator.uiautomator()
        if xml is None:
            logger.warning('Something wrong with processing - getting None Type from Websocket...')
            return ScreenType.ERROR
        try:
            parser = ET.XMLParser(encoding="utf-8")
            xmlroot = ET.fromstring(xml, parser=parser)
            bounds: str = ""
            accept_x: Optional[int] = None
            accept_y: Optional[int] = None
            # On some resolutions (100, 100) position that MAD clicks by default to close keyboard
            # ended on clicking Firefox SSL certifcate icon and it nuked whole flow by opening something else
            # Changing it to (300, 300), but also detecting big logo image on website and taking this as new coords
            exit_keyboard_x: int = 300
            exit_keyboard_y: int = 300

            for item in xmlroot.iter('node'):
                if "Access denied" in item.attrib["text"]:
                    logger.warning("WAF on PTC login attempt detected")
                    # Reload the page 1-3 times
                    for i in range(random.randint(1, 3)):
                        logger.info("Reload #{}", i)
                        await self.__handle_ptc_waf()
                    return ScreenType.PTC
                elif item.attrib["class"] == "android.widget.Image":
                    bounds = item.attrib['bounds']
                    match = re.search(r'^\[(\d+),(\d+)\]\[(\d+),(\d+)\]$', bounds)
                    logger.debug("Logo image Bounds {}", item.attrib['bounds'])
                    exit_keyboard_x = int(int(match.group(1)) + ((int(match.group(3)) - int(match.group(1))) / 2))
                    exit_keyboard_y = int(int(match.group(2)) + ((int(match.group(4)) - int(match.group(2))) / 2))
                elif (item.attrib["resource-id"] == "email"
                      or ("EditText" in item.attrib["class"] and item.attrib["index"] == "0")):
                    bounds = item.attrib['bounds']
                    logger.info("Found email/login field, clicking, filling, clicking")
                    logger.debug("email-node Bounds {}", item.attrib['bounds'])
                    match = re.search(r'^\[(\d+),(\d+)\]\[(\d+),(\d+)\]$', bounds)
                    click_x = int(match.group(1)) + ((int(match.group(3)) - int(match.group(1))) / 2)
                    click_y = int(match.group(2)) + ((int(match.group(4)) - int(match.group(2))) / 2)
                    await self._communicator.click(int(click_x), int(click_y))
                    await asyncio.sleep(2)
                    await self._communicator.enter_text(self._worker_state.active_account.username)
                    await self._communicator.click(exit_keyboard_x, exit_keyboard_y)
                    await asyncio.sleep(2)
                elif (item.attrib["resource-id"] == "password"
                      or ("EditText" in item.attrib["class"] and item.attrib["index"] == "1")):
                    bounds = item.attrib['bounds']
                    logger.debug("password-node Bounds {}", item.attrib['bounds'])
                    logger.info("Found password field, clicking, filling, clicking")
                    match = re.search(r'^\[(\d+),(\d+)\]\[(\d+),(\d+)\]$', bounds)
                    click_x = int(match.group(1)) + ((int(match.group(3)) - int(match.group(1))) / 2)
                    click_y = int(match.group(2)) + ((int(match.group(4)) - int(match.group(2))) / 2)
                    await self._communicator.click(int(click_x), int(click_y))
                    await asyncio.sleep(2)
                    await self._communicator.enter_text(self._worker_state.active_account.password)
                    await self._communicator.click(exit_keyboard_x, exit_keyboard_y)
                    await asyncio.sleep(2)
                elif "Button" in item.attrib["class"] and (item.attrib["resource-id"] == "accept"
                                                           or item.attrib["text"] in ("Anmelden", "Log In")):
                    bounds = item.attrib['bounds']
                    logger.info("Found Log In button")
                    logger.debug("accept-node Bounds {}", item.attrib['bounds'])
                    match = re.search(r'^\[(\d+),(\d+)\]\[(\d+),(\d+)\]$', bounds)
                    accept_x = int(int(match.group(1)) + ((int(match.group(3)) - int(match.group(1))) / 2))
                    accept_y = int(int(match.group(2)) + ((int(match.group(4)) - int(match.group(2))) / 2))
                    # button for actual login
                    if MadGlobals.application_args.enable_login_tracking and self._worker_state.active_account.login_type == LoginType.ptc.name:
                        # Check whether a PTC login rate limit applies before trying to login using credentials as this may trigger
                        # just as a plain startup of already logged in account/device
                        logger.debug("Login tracking enabled")
                        if not await self.check_ptc_login_ban(increment_count=True):
                            logger.warning("Potential PTC ban, aborting PTC login for now. Sleeping 30s")
                            await asyncio.sleep(30)
                            await self._communicator.stop_app("com.nianticlabs.pokemongo")
                            return ScreenType.ERROR
                        else:
                            await self._mapping_manager.login_tracking_remove_value(origin=self._worker_state.origin)
                            logger.success("Received permission for (potential) PTC login")
            if accept_x and accept_y:
                await self._communicator.click(accept_x, accept_y)
                logger.info("Clicking Log In and sleeping 50 seconds - please wait!")
                await asyncio.sleep(50)
                if await self.get_devicesettings_value(MappingManagerDevicemappingKey.EXTENDED_LOGIN, False):
                    # Start pogodroid service again to make sure we are running PD properly here
                    await self._communicator.passthrough(
                        "su -c 'am broadcast -a com.mad.pogodroid.SET_INTENTIONAL_STOP -c android.intent.category.DEFAULT -n com.mad.pogodroid/.IntentionalStopSetterReceiver --ez value false'")
                    await asyncio.sleep(2)
                    await self._communicator.passthrough(
                        "su -c 'am start-foreground-service -n com.mad.pogodroid/.services.HookReceiverService'")
                    await asyncio.sleep(5)
                    await self._communicator.stop_app("com.nianticlabs.pokemongo")
                    await asyncio.sleep(10)
                    await self._communicator.start_app("com.nianticlabs.pokemongo")
                    await asyncio.sleep(120)
                return ScreenType.PTC
            else:
                logger.error("Log in [accept] button not found?")
                return ScreenType.ERROR

        except Exception as e:
            logger.error('Something wrong while parsing xml: {}', e)
            logger.exception(e)
            return ScreenType.ERROR

    async def __handle_failure_screen(self) -> None:
        await self.__handle_returning_player_or_wrong_credentials()

    async def __handle_ggl_consent_screen(self) -> ScreenType:
        if (self._worker_state.resolution_calculator.screen_size_x == 0
                and self._worker_state.resolution_calculator.screen_size_y == 0):
            logger.warning("Screen width and height are zero - try to get real values from new screenshot ...")
            # this updates screen sizes etc
            result = await self._take_and_analyze_screenshot()
            if not result:
                logger.error("Failed getting/analyzing screenshot")
                return ScreenType.ERROR
        if ((self._worker_state.resolution_calculator.screen_size_x != 720
             and self._worker_state.resolution_calculator.screen_size_y != 1280)
                and (self._worker_state.resolution_calculator.screen_size_x != 1080
                     and self._worker_state.resolution_calculator.screen_size_y != 1920)
                and (self._worker_state.resolution_calculator.screen_size_x != 1440
                     and self._worker_state.resolution_calculator.screen_size_y != 2560)):
            logger.warning("The google consent screen can only be handled on 720x1280, 1080x1920 and 1440x2560 screens "
                           "(width is {}, height is {})",
                           self._worker_state.resolution_calculator.screen_size_x,
                           self._worker_state.resolution_calculator.screen_size_y)
            return ScreenType.ERROR
        logger.info("Click accept button")
        if self._worker_state.resolution_calculator.screen_size_x == 720 and self._worker_state.resolution_calculator.screen_size_y == 1280:
            await self._communicator.touch_and_hold(int(360), int(1080), int(360), int(500))
            await self._communicator.click(480, 1080)
        if self._worker_state.resolution_calculator.screen_size_x == 1080 and self._worker_state.resolution_calculator.screen_size_y == 1920:
            await self._communicator.touch_and_hold(int(360), int(1800), int(360), int(400))
            await self._communicator.click(830, 1638)
        if self._worker_state.resolution_calculator.screen_size_x == 1440 and self._worker_state.resolution_calculator.screen_size_y == 2560:
            await self._communicator.touch_and_hold(int(360), int(2100), int(360), int(400))
            await self._communicator.click(976, 2180)
        await asyncio.sleep(10)
        return ScreenType.UNDEFINED

    async def __handle_returning_player_or_wrong_credentials(self) -> None:
        self._nextscreen = ScreenType.UNDEFINED
        screenshot_path = await self.get_screenshot_path()
        coordinates: Optional[ScreenCoordinates] = await self._worker_state.pogo_windows.look_for_button(
            screenshot_path,
            2.20, 3.01,
            upper=True)
        if coordinates:
            coordinates = ScreenCoordinates(int(self._worker_state.resolution_calculator.screen_size_x / 2),
                                            int(self._worker_state.resolution_calculator.screen_size_y * 0.7 - self._worker_state.resolution_calculator.y_offset))
            await self._communicator.click(coordinates.x, coordinates.y)
            await asyncio.sleep(2)

    async def __handle_birthday_screen(self) -> None:
        if await self.get_devicesettings_value(MappingManagerDevicemappingKey.EXTENDED_LOGIN, False):
            logger.info("Extended login, stopping PD entirely and restarting POGO.")
            # First disable pogodroid at this point to avoid the injection triggering any checks in other libraries
            await self._communicator.passthrough(
                "su -c 'am broadcast -a com.mad.pogodroid.SET_INTENTIONAL_STOP -c android.intent.category.DEFAULT -n com.mad.pogodroid/.IntentionalStopSetterReceiver --ez value true'")
            await asyncio.sleep(5)
            await self._communicator.passthrough(
                "su -c 'am stopservice -n com.mad.pogodroid/.services.HookReceiverService'")
            await self._communicator.stop_app("com.nianticlabs.pokemongo")
            await asyncio.sleep(10)
            await self._communicator.start_app("com.nianticlabs.pokemongo")
        await asyncio.sleep(30)

        # After having restarted pogo, we should again be on the birthday screen now and PD is turned off
        self._nextscreen = ScreenType.RETURNING
        click_x = int((self._worker_state.resolution_calculator.screen_size_x / 2) + (
                self._worker_state.resolution_calculator.screen_size_x / 4))
        click_y = int(self._worker_state.resolution_calculator.screen_size_y / 1.69)
        await self._communicator.click(click_x, click_y)
        await self._communicator.touch_and_hold(click_x, click_y, click_x, int(click_y - (
                self._worker_state.resolution_calculator.screen_size_y / 2)), 200)
        await asyncio.sleep(1)
        await self._communicator.touch_and_hold(click_x, click_y, click_x, int(click_y - (
                self._worker_state.resolution_calculator.screen_size_y / 2)), 200)
        await asyncio.sleep(1)
        await self._communicator.click(click_x, click_y)
        await asyncio.sleep(1)
        click_x = int(self._worker_state.resolution_calculator.screen_size_x / 2)
        click_y = int(click_y + (self._worker_state.resolution_calculator.screen_size_y / 8.53))
        await self._communicator.click(click_x, click_y)
        await asyncio.sleep(1)

    async def __handle_welcome_screen(self) -> ScreenType:
        screenshot_path = await self.get_screenshot_path()
        coordinates: Optional[ScreenCoordinates] = await self._worker_state.pogo_windows.look_for_button(
            screenshot_path,
            2.20, 3.01,
            upper=True)
        if coordinates:
            await self._communicator.click(coordinates.x, coordinates.y)
            await asyncio.sleep(2)
            return ScreenType.TOS
        return ScreenType.NOTRESPONDING

    async def __handle_tos_screen(self) -> ScreenType:
        screenshot_path = await self.get_screenshot_path()
        await self._communicator.click(int(self._worker_state.resolution_calculator.screen_size_x / 2),
                                       int(self._worker_state.resolution_calculator.screen_size_y * 0.47))
        coordinates: Optional[ScreenCoordinates] = await self._worker_state.pogo_windows.look_for_button(
            screenshot_path,
            2.20, 3.01,
            upper=True)
        if coordinates:
            await self._communicator.click(coordinates.x, coordinates.y)
            await asyncio.sleep(2)
            return ScreenType.PRIVACY
        return ScreenType.NOTRESPONDING

    async def __handle_privacy_screen(self) -> ScreenType:
        screenshot_path = await self.get_screenshot_path()
        coordinates: Optional[ScreenCoordinates] = await self._worker_state.pogo_windows.look_for_button(
            screenshot_path,
            2.20, 3.01,
            upper=True)
        if coordinates:
            await self._communicator.click(coordinates.x, coordinates.y)
            await asyncio.sleep(3)
            return ScreenType.WILLOWCHAR
        return ScreenType.NOTRESPONDING

    async def __handle_character_selection_screen(self) -> ScreenType:
        for _ in range(9):
            await self._communicator.click(100, 100)
            await asyncio.sleep(1)
        await asyncio.sleep(1)
        await self._communicator.click(int(self._worker_state.resolution_calculator.screen_size_x / 4),
                                       int(self._worker_state.resolution_calculator.screen_size_y / 2))
        await asyncio.sleep(2)
        for _ in range(3):
            await self._communicator.click(int(self._worker_state.resolution_calculator.screen_size_x * 0.91),
                                           int(self._worker_state.resolution_calculator.screen_size_y * 0.94))
            await asyncio.sleep(2)

        if not await self._take_screenshot(delay_before=await self.get_devicesettings_value(
                MappingManagerDevicemappingKey.POST_SCREENSHOT_DELAY, 1),
                                           delay_after=2):
            logger.error("Failed getting screenshot")
            return ScreenType.ERROR

        screenshot_path = await self.get_screenshot_path()
        coordinates: Optional[ScreenCoordinates] = await self._worker_state.pogo_windows.look_for_button(
            screenshot_path,
            2.20, 3.01,
            upper=True)
        if coordinates:
            await self._communicator.click(coordinates.x, coordinates.y)
            await asyncio.sleep(5)
            return ScreenType.WILLOWCATCH
        return ScreenType.NOTRESPONDING

    async def __handle_catch_tutorial(self) -> ScreenType:
        for _ in range(2):
            await self._communicator.click(100, 100)
        for x in range(1, 10):
            for y in range(1, 10):
                click_x = int(self._worker_state.resolution_calculator.screen_size_x * x / 10)
                click_y = int(
                    self._worker_state.resolution_calculator.screen_size_y * y / 20 + self._worker_state.resolution_calculator.screen_size_y / 2)
                await self._communicator.click(click_x, click_y)
            await asyncio.sleep(5)
            if not await self._take_screenshot(delay_before=await self.get_devicesettings_value(
                    MappingManagerDevicemappingKey.POST_SCREENSHOT_DELAY, 1),
                                               delay_after=2):
                logger.error("Failed getting screenshot")
                return ScreenType.ERROR
            screenshot_path = await self.get_screenshot_path()
            globaldict = await self._worker_state.pogo_windows.get_screen_text(screenshot_path,
                                                                               self._worker_state.origin)
            starter = ['Bulbasaur', 'Charmander', 'Squirtle', 'Bisasam', 'Glumanda', 'Schiggy', 'Bulbizarre',
                       'Salameche', 'Carapuce']
            if any(text in starter for text in globaldict['text']):
                logger.debug("Found Pokémon")
                break

        for _ in range(3):
            click_x = int(self._worker_state.resolution_calculator.screen_size_x / 2)
            click_y = int(self._worker_state.resolution_calculator.screen_size_y * 0.93)
            await self._communicator.touch_and_hold(click_x, click_y, click_x, int(click_y - (
                    self._worker_state.resolution_calculator.screen_size_y / 2)), 200)
            await asyncio.sleep(15)

            if not await self._take_screenshot(delay_before=await self.get_devicesettings_value(
                    MappingManagerDevicemappingKey.POST_SCREENSHOT_DELAY, 1), delay_after=2):
                logger.error("Failed getting screenshot")
                return ScreenType.ERROR

            screenshot_path = await self.get_screenshot_path()
            coordinates: Optional[ScreenCoordinates] = await self._worker_state.pogo_windows.look_for_button(
                screenshot_path,
                2.20, 3.01,
                upper=True)
            if coordinates:
                await self._communicator.click(coordinates.x, coordinates.y)
                logger.info("Catched Pokémon.")
                await asyncio.sleep(12)
                await self._communicator.click(int(self._worker_state.resolution_calculator.screen_size_x / 2),
                                               int(self._worker_state.resolution_calculator.screen_size_y * 0.93))
                await asyncio.sleep(2)
                return ScreenType.UNDEFINED

        logger.warning("Could not catch Pokémon.")
        return ScreenType.NOTRESPONDING

    async def __handle_name_screen(self) -> ScreenType:
        for _ in range(2):
            await self._communicator.click(100, 100)
            await asyncio.sleep(1)
        await asyncio.sleep(5)

        if not self._worker_state.active_account:
            logger.error('No PTC Username and Password is set')
            return ScreenType.ERROR
        username = self._worker_state.active_account.username
        logger.debug('Setting name for Account to {}', username)
        await self._communicator.enter_text(username)
        await self._communicator.click(100, 100)
        await asyncio.sleep(2)
        await self._communicator.click(int(self._worker_state.resolution_calculator.screen_size_x / 2),
                                       int(self._worker_state.resolution_calculator.screen_size_y * 0.66))
        await self._communicator.click(int(self._worker_state.resolution_calculator.screen_size_x / 2),
                                       int(self._worker_state.resolution_calculator.screen_size_y * 0.51))
        await asyncio.sleep(2)

        if not await self._take_screenshot(delay_before=await self.get_devicesettings_value(
                MappingManagerDevicemappingKey.POST_SCREENSHOT_DELAY, 1),
                                           delay_after=2):
            logger.error("Failed getting screenshot")
            return ScreenType.ERROR
        screenshot_path = await self.get_screenshot_path()
        globaldict = await self._worker_state.pogo_windows.get_screen_text(screenshot_path, self._worker_state.origin)
        errortext = ['available.', 'verfugbar.', 'disponible.']
        if any(text in errortext for text in globaldict['text']):
            logger.warning('Account name is not available. Marking account as permabanned!')
            await self._account_handler.mark_burnt(self._worker_state.device_id,
                                                   BurnType.BAN)
            return ScreenType.MAINTENANCE

        await self._communicator.click(100, 100)
        await self._communicator.click(100, 100)
        await asyncio.sleep(5)
        return ScreenType.ADVENTURESYNC

    async def __handle_adventure_sync_screen(self, screentype: ScreenType) -> ScreenType:
        if not await self.parse_adventure_sync(await self._communicator.uiautomator()):
            screentype = ScreenType.ERROR
        await asyncio.sleep(5)
        return screentype

    async def __handle_tutorial_end(self) -> ScreenType:
        for _ in range(4):
            await self._communicator.click(100, 100)
        await asyncio.sleep(1)
        return ScreenType.POGO

    async def detect_screentype(self, y_offset: int = 0) -> ScreenType:
        topmostapp = await self._communicator.topmost_app()
        if not topmostapp:
            logger.warning("Failed getting the topmost app!")
            return ScreenType.ERROR

        screentype, global_dict, diff = await self.__evaluate_topmost_app(topmost_app=topmostapp)
        logger.info("Processing Screen: {}", str(ScreenType(screentype)))
        return await self.__handle_screentype(screentype=screentype, global_dict=global_dict, diff=diff,
                                              y_offset=y_offset)

    async def check_quest(self, screenpath: str) -> ScreenType:
        if screenpath is None or len(screenpath) == 0:
            logger.error("Invalid screen path: {}", screenpath)
            return ScreenType.ERROR
        globaldict = await self._worker_state.pogo_windows.get_screen_text(screenpath, self._worker_state.origin)

        click_text = 'FIELD,SPECIAL,FELD,SPEZIAL,SPECIALES,TERRAIN'
        if not globaldict:
            # dict is empty
            return ScreenType.ERROR
        n_boxes = len(globaldict['text'])
        for i in range(n_boxes):
            if any(elem in (globaldict['text'][i]) for elem in click_text.split(",")):
                logger.info('Found research menu')
                await self._communicator.click(100, 100)
                return ScreenType.QUEST

        logger.info('Listening to Dr. blabla - please wait')

        await self._communicator.back_button()
        await asyncio.sleep(3)
        return ScreenType.UNDEFINED

    async def parse_adventure_sync(self, xml) -> bool:
        if xml is None:
            logger.warning('Something wrong with processing - getting None Type from Websocket...')
            return False
        click_text = ('MAYBE LATER', 'VIELLEICHT SPATER', 'PEUT-ETRE PLUS TARD', 'OK')
        try:
            parser = ET.XMLParser(encoding="utf-8")
            xmlroot = ET.fromstring(xml, parser=parser)
            bounds: str = ""
            for item in xmlroot.iter('node'):
                logger.debug(str(item.attrib['text']))
                if str(item.attrib['text']).upper() in click_text:
                    logger.debug("Found text {}", item.attrib['text'])
                    bounds = item.attrib['bounds']
                    logger.debug("Bounds {}", item.attrib['bounds'])

                    match = re.search(r'^\[(\d+),(\d+)\]\[(\d+),(\d+)\]$', bounds)

                    click_x = int(match.group(1)) + ((int(match.group(3)) - int(match.group(1))) / 2)
                    click_y = int(match.group(2)) + ((int(match.group(4)) - int(match.group(2))) / 2)
                    await self._communicator.click(int(click_x), int(click_y))
                    await asyncio.sleep(5)
                    return True
        except Exception as e:
            logger.error('Something wrong while parsing xml: {}', e)
            logger.exception(e)
            return False

        await asyncio.sleep(2)
        logger.warning('Could not find any button...')
        return False

    async def parse_permission(self, xml) -> bool:
        if xml is None:
            logger.warning('Something wrong with processing - getting None Type from Websocket...')
            return False
        click_text = ('ZUGRIFF NUR', 'ZULASSEN', 'ALLOW', 'AUTORISER', 'OK')
        try:
            parser = ET.XMLParser(encoding="utf-8")
            xmlroot = ET.fromstring(xml, parser=parser)
            bounds: str = ""
            found_nodes: List = []
            for item in xmlroot.iter('node'):
                text_upper: str = str(item.attrib['text']).upper()
                res = [ele for ele in click_text if (ele in text_upper)]
                if res:
                    found_nodes.append(item)
            found_nodes.reverse()
            for node in found_nodes:
                logger.debug("Found text {}", node.attrib['text'])
                bounds = node.attrib['bounds']
                logger.debug("Bounds {}", node.attrib['bounds'])

                match = re.search(r'^\[(\d+),(\d+)\]\[(\d+),(\d+)\]$', bounds)

                click_x = int(match.group(1)) + ((int(match.group(3)) - int(match.group(1))) / 2)
                click_y = int(match.group(2)) + ((int(match.group(4)) - int(match.group(2))) / 2)
                await self._communicator.click(int(click_x), int(click_y))
                await asyncio.sleep(2)
                return True
        except Exception as e:
            logger.error('Something wrong while parsing xml: {}', e)
            logger.exception(e)
            return False

        await asyncio.sleep(2)
        logger.warning('Could not find any button...')
        return False

    async def parse_ggl(self, xml, mails: List[str]) -> bool:
        if xml is None:
            logger.warning('Something wrong with processing - getting None Type from Websocket...')
            return False
        try:
            parser = ET.XMLParser(encoding="utf-8")
            xmlroot = ET.fromstring(xml, parser=parser)
            for item in xmlroot.iter('node'):
                for mail in mails:
                    if (mail and mail.lower() in str(item.attrib['text']).lower()
                            or not mail and (item.attrib["resource-id"] == "com.google.android.gms:id/account_name"
                                             or "@" in str(item.attrib['text']))):
                        logger.info("Found mail {}", self.censor_account(str(item.attrib['text'])))
                        bounds = item.attrib['bounds']
                        logger.debug("Bounds {}", item.attrib['bounds'])
                        match = re.search(r'^\[(\d+),(\d+)\]\[(\d+),(\d+)\]$', bounds)
                        click_x = int(match.group(1)) + ((int(match.group(3)) - int(match.group(1))) / 2)
                        click_y = int(match.group(2)) + ((int(match.group(4)) - int(match.group(2))) / 2)
                        await self._communicator.click(int(click_x), int(click_y))
                        await asyncio.sleep(5)
                        return True
        except Exception as e:
            logger.error('Something wrong while parsing xml: {}', e)
            logger.exception(e)
            return False

        await asyncio.sleep(2)
        logger.warning('Dont find any mailaddress...')
        return False

    async def set_devicesettings_value(self, key: MappingManagerDevicemappingKey, value) -> None:
        await self._mapping_manager.set_devicesetting_value_of(self._worker_state.origin, key, value)

    async def get_devicesettings_value(self, key: MappingManagerDevicemappingKey, default_value: object = None):
        logger.debug2("Fetching devicemappings")
        try:
            value = await self._mapping_manager.get_devicesetting_value_of_device(self._worker_state.origin, key)
        except (EOFError, FileNotFoundError) as e:
            logger.warning("Failed fetching devicemappings in worker with description: {}. Stopping worker", e)
            return None
        return value if value is not None else default_value

    def censor_account(self, emailaddress, is_ptc=False):
        # PTC account
        if is_ptc:
            return (emailaddress[0:2] + "***" + emailaddress[-2:])
        # GGL - make sure we have @ there.
        # If not it could be wrong match, so returning original
        if '@' in emailaddress:
            user, domain = emailaddress.split("@", 1)
            # long local-part, censor middle part only
            if len(user) > 6:
                return (user[0:2] + "***" + user[-2:] + "@" + domain)
            # domain only, just return
            elif len(user) == 0:
                return (emailaddress)
            # local-part is short, asterix for each char
            else:
                return ("*" * len(user) + "@" + domain)
        return emailaddress

    async def get_screenshot_path(self, fileaddon: bool = False) -> str:
        screenshot_ending: str = ".jpg"
        addon: str = ""
        if await self.get_devicesettings_value(MappingManagerDevicemappingKey.SCREENSHOT_TYPE, "jpeg") == "png":
            screenshot_ending = ".png"

        if fileaddon:
            addon: str = "_" + str(time.time())

        screenshot_filename = "screenshot_{}{}{}".format(str(self._worker_state.origin), str(addon), screenshot_ending)

        if fileaddon:
            logger.info("Creating debugscreen: {}", screenshot_filename)

        return os.path.join(
            MadGlobals.application_args.temp_path, screenshot_filename)

    async def _take_screenshot(self, delay_after=0.0, delay_before=0.0, errorscreen: bool = False):
        logger.debug("Taking screenshot...")
        await asyncio.sleep(delay_before)

        # TODO: area settings for jpg/png and quality?
        screenshot_type: ScreenshotType = ScreenshotType.JPEG
        if await self.get_devicesettings_value(MappingManagerDevicemappingKey.SCREENSHOT_TYPE, "jpeg") == "png":
            screenshot_type = ScreenshotType.PNG

        screenshot_quality: int = 80

        take_screenshot = await self._communicator.get_screenshot(await self.get_screenshot_path(fileaddon=errorscreen),
                                                                  screenshot_quality, screenshot_type)

        if not take_screenshot:
            logger.error("takeScreenshot: Failed retrieving screenshot")
            logger.debug("Failed retrieving screenshot")
            return False
        else:
            logger.debug("Success retrieving screenshot")
            self._lastScreenshotTaken = time.time()
            await asyncio.sleep(delay_after)
            return True

    async def __handle_login_timeout(self, diff, global_dict) -> None:
        self._nextscreen = ScreenType.UNDEFINED
        click_text = 'SIGNOUT,SIGN,ABMELDEN,_DECONNECTER'
        await self.__click_center_button_text(click_text, diff, global_dict)

    async def _take_and_analyze_screenshot(self, delay_after=0.0, delay_before=0.0, errorscreen: bool = False) -> \
            Optional[Tuple[ScreenType,
            Optional[
                dict], int]]:
        if not await self._take_screenshot(delay_before=await self.get_devicesettings_value(
                MappingManagerDevicemappingKey.POST_SCREENSHOT_DELAY, 1),
                                           delay_after=2):
            logger.error("Failed getting screenshot")
            return None

        screenpath = await self.get_screenshot_path()

        result: Optional[Tuple[ScreenType,
        Optional[
            dict], int, int, int]] = await self._worker_state.pogo_windows \
            .screendetection_get_type_by_screen_analysis(screenpath, self._worker_state.origin)
        if result is None:
            logger.error("Failed analyzing screen")
            return None
        else:
            returntype, global_dict, width, height, diff = result
            self._worker_state.resolution_calculator.screen_size_x = width
            self._worker_state.resolution_calculator.screen_size_y = height
            return returntype, global_dict, diff

    async def clear_game_data(self):
        await self._communicator.reset_app_data("com.nianticlabs.pokemongo")
        await self._account_handler.notify_logout(self._worker_state.device_id)
        self._worker_state.active_account = None
        # TODO: Immediately assign a new account?

    async def __handle_ptc_waf(self) -> None:
        """
        The WAF was either triggered at random (happens) and a simple reload is needed or the IP was blacklisted.
        Let's pull down the page to trigger a reload.
        """
        # First fetch the bounds, then randomly pick an X coordinate roughly around the center +-10% (random)
        # Then, fetch y of upper 10-20% part
        # swipe down for half the screen with mildly varying X coordinate as target to randomize swipes
        center_x: int = int(self._worker_state.resolution_calculator.screen_size_x / 2)
        upper_x: int = random.randint(int(center_x * 0.9), int(center_x * 1.1))
        lower_x: int = random.randint(int(center_x * 0.9), int(center_x * 1.1))

        upper_y: int = random.randint(int(self._worker_state.resolution_calculator.screen_size_y * 0.2),
                                      int(self._worker_state.resolution_calculator.screen_size_y * 0.35))
        lower_y: int = random.randint(int(self._worker_state.resolution_calculator.screen_size_y * 0.5),
                                      int(self._worker_state.resolution_calculator.screen_size_y * 0.65))
        swipe_duration: int = random.randint(800, 1500)
        await self._communicator.touch_and_hold(upper_x, upper_y, lower_x, lower_y, swipe_duration)
        # Returning ScreenType PTC for now to re-evaluate
