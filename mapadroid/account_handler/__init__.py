import time
from typing import Optional

from loguru import logger

from mapadroid.account_handler.AbstractAccountHandler import \
    AbstractAccountHandler
from mapadroid.account_handler.AccountHandler import AccountHandler
from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.db.model import SettingsPogoauth
from mapadroid.geofence.geofenceHelper import GeofenceHelper
from mapadroid.mapping_manager.MappingManager import MappingManager
from mapadroid.utils.collections import Location
from mapadroid.worker.WorkerState import WorkerState


async def setup_account_handler(db_wrapper: DbWrapper) -> AbstractAccountHandler:
    """
    Utility method to be extended/overwritten for any other account handling options (e.g., external provider)
    Args:
        db_wrapper:

    Returns:

    """
    return AccountHandler(db_wrapper)


async def fetch_auth_details(mapping_manager: MappingManager,
                             worker_state: WorkerState, account_handler: AbstractAccountHandler) -> None:
    logger.debug("Checking for a new account")
    last_used_account_valid: bool = False
    try:
        last_used_account_valid = (worker_state.active_account
                                   and await account_handler.is_burnt(worker_state.device_id,
                                                                      worker_state.active_account.account_id))
    except ValueError as e:
        logger.warning("Account last used does not match the assignment of accounts stored in DB")
        worker_state.active_account = None
    if worker_state.active_account_last_set + 300 < time.time() or not last_used_account_valid:
        logger.info("Detected login screen, fetching new account to use since last account was assigned more "
                    "than 5 minutes ago OR current account was marked burnt")
        location_to_scan: Optional[Location] = None
        if not location_to_scan \
                or worker_state.current_location.lat == 0 and worker_state.current_location.lng == 0:
            # Default location, use the middle of the geofence...
            geofence_helper: Optional[GeofenceHelper] = await mapping_manager \
                .routemanager_get_geofence_helper(worker_state.area_id)
            if geofence_helper:
                lat, lon = geofence_helper.get_middle_from_fence()
                location_to_scan = Location(lat, lon)
        else:
            location_to_scan = worker_state.current_location

        account_to_use: Optional[SettingsPogoauth] = await account_handler.get_account(
            worker_state.device_id,
            await mapping_manager.routemanager_get_purpose_of_device(worker_state.area_id),
            location_to_scan
        )
        if not account_to_use:
            logger.error("No account to use found, are there too few accounts in DB or did MAD screw up here? "
                         "Please make sure accounts in MADmin->Settings->Pogo Auth have correct level set - edit "
                         "it manually if imported with 0/1 - MAD does not (auto)login to check levels "
                         "(unless levelmode is active.")
            worker_state.active_account = None
        else:
            logger.info("Account for {}: {}", worker_state.origin, account_to_use.username)
            worker_state.active_account = account_to_use
    else:
        logger.info("Account was set recently and is still assigned to device {} in DB")
