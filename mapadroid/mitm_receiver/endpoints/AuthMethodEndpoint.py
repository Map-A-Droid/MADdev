from typing import List, Optional

from loguru import logger

from mapadroid.db.helper.SettingsDeviceHelper import SettingsDeviceHelper
from mapadroid.db.helper.SettingsPogoauthHelper import SettingsPogoauthHelper
from mapadroid.db.model import SettingsDevice, SettingsPogoauth
from mapadroid.mitm_receiver.endpoints.AbstractMitmReceiverRootEndpoint import \
    AbstractMitmReceiverRootEndpoint


class AuthMethodEndpoint(AbstractMitmReceiverRootEndpoint):
    """
    "/auth_method"
    """

    # TODO: Check Origin etc auth checks using decorators or better: visitor pattern...

    async def _iter(self):
        # TODO: VisitorPattern for extra auth checks...
        with logger.contextualize(identifier=self._get_request_address(), name="auth-method-endpoint"):
            await self._check_origin_header()
            return await super()._iter()

    async def get(self):
        origin = self.request.headers.get("Origin")
        if not origin:
            logger.error("Request without origin header")
            return self._json_response({})
        # TODO: Fetch assigned via API of AccountHandler implementation allowing for RPC
        async with self._get_db_wrapper() as session, session:
            device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get_by_origin(
                session, self._get_instance_id(), origin)
            if not device_entry:
                logger.warning("Device origin {} not found in device table", origin)
                return self._json_response({})
            currently_assigned: Optional[SettingsPogoauth] = await SettingsPogoauthHelper.get_assigned_to_device(
                session, device_entry.device_id)
            if not currently_assigned:
                logger.warning("No auth assigned to device {}", origin)
            response = {"method": str(currently_assigned.login_type) if currently_assigned else ""}
            logger.success("Instructing {} to use {}", origin, response["method"])
            return self._json_response(response)
