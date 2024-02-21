import datetime
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from mapadroid.db.model import SettingsPogoauth
from mapadroid.utils.collections import Location


class AccountPurpose(Enum):
    IV_QUEST = "quest_iv"
    LEVEL = "level"
    QUEST = "quest"
    IV = "iv"
    MON_RAID = "mon_raid"


class BurnType(Enum):
    BAN = "ban"
    SUSPENDED = "suspended"
    MAINTENANCE = "maintenance"


class AbstractAccountHandler(ABC):
    @abstractmethod
    async def get_account(self, device_id: int, purpose: AccountPurpose,
                          location_to_scan: Optional[Location],
                          including_google: bool = True) -> Optional[SettingsPogoauth]:
        """
        Searches for an available (i.e., known to not be cooling down) account in the settings_pogoauth table
        Returns: a pogoauth entry to be used to which the worker can try to log in

        """
        pass

    @abstractmethod
    async def notify_logout(self, device_id: int) -> None:
        """
        Upon clearing game data, a logout can be notified. E.g., to release a device_id mapping to an account
        Args:
            device_id:

        Returns:

        """
        pass

    @abstractmethod
    async def mark_burnt(self, device_id: int, burn_type: Optional[BurnType]) -> None:
        """
        Marks the account to which the device identified by the device_id is bound to as burnt with the time of calling
        Returns:

        """
        pass

    @abstractmethod
    async def set_last_softban_action(self, device_id: int, time_of_action: datetime.datetime,
                                      location_of_action: Location) -> None:
        """
        Stores the information on when the last softban action for the account currently associated to device was
        performed.
        Args:
            device_id:
            time_of_action:
            location_of_action:

        Returns:

        """
        pass

    @abstractmethod
    async def set_level(self, device_id: int, level: int) -> None:
        """
        Sets the level associated to the device ID
        Args:
            device_id: settings_device.device_id to which the settings_pogoauth is linked with
            level: level to be stored in the database

        Returns:

        """
        pass

    @abstractmethod
    async def get_assigned_username(self, device_id: int) -> Optional[str]:
        """
        Args:
            device_id:

        Returns: the username to which the origin (i.e., device) last received credentials to log in with

        """
        pass

    @abstractmethod
    async def is_burnt(self, device_id: int, account_id: Optional[int] = None) -> bool:
        """

        Args:
            device_id: The device_id to which an account may be assigned
            account_id: Optionally the account id of the entry expected

        Returns: False if no auth has an assignment to the device or the auth entry was found not to be burnt

        """
        pass
