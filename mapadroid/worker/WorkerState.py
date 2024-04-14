import asyncio
import time
from datetime import datetime
from typing import Optional

from mapadroid.db.model import SettingsPogoauth
from mapadroid.ocr.pogoWindows import PogoWindows
from mapadroid.ocr.screen_type import ScreenType
from mapadroid.utils.collections import Location
from mapadroid.utils.madConstants import TIMESTAMP_NEVER
from mapadroid.utils.madGlobals import TransportType
from mapadroid.utils.pogoevent import PogoEvent
from mapadroid.utils.resolution import ResolutionCalculator


class WorkerState:
    def __init__(self, origin: str, device_id: int, stop_worker_event: asyncio.Event, active_event: PogoEvent,
                 pogo_windows: PogoWindows, current_auth: Optional[SettingsPogoauth]):
        self.device_id: int = device_id
        self.origin: str = origin
        self.stop_worker_event: asyncio.Event = stop_worker_event
        self.resolution_calculator: ResolutionCalculator = ResolutionCalculator()
        self.pogo_windows: PogoWindows = pogo_windows
        self.active_event: PogoEvent = active_event
        # Optional in case it's not known as per DB...
        self._active_account: Optional[SettingsPogoauth] = current_auth
        # Stores the time an account was last assigned. Avoid assigning accounts all the time
        self._active_account_last_set: int = 0
        self.maintenance_early_detection_triggered: bool = False
        self.area_id: Optional[int] = None

        self.current_location: Optional[Location] = Location(0.0, 0.0)
        self.last_location: Optional[Location] = Location(0.0, 0.0)
        self.location_count: int = 0
        self.login_error_count: int = 0
        self.last_transport_type: TransportType = TransportType.TELEPORT
        self.last_screenshot_taken_at: int = TIMESTAMP_NEVER
        self.last_screen_type: ScreenType = ScreenType.UNDEFINED
        self.current_sleep_duration: int = 0
        self.last_received_data_time: Optional[datetime] = None
        self.restart_count: int = 0
        self.reboot_count: int = 0
        self.same_screen_count: int = 0

    @property
    def active_account(self) -> Optional[SettingsPogoauth]:
        return self._active_account

    @active_account.setter
    def active_account(self, value: Optional[SettingsPogoauth]) -> None:
        if not value:
            self._active_account_last_set = 0
        else:
            self._active_account_last_set = int(time.time())
        self.active_event = value

    @property
    def active_account_last_set(self) -> int:
        return self._active_account_last_set
