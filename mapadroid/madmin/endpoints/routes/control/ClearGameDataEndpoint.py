from typing import Optional

from aiohttp import web
from loguru import logger

from mapadroid.madmin.endpoints.routes.control.AbstractControlEndpoint import \
    AbstractControlEndpoint
from mapadroid.mapping_manager.MappingManager import DeviceMappingsEntry
from mapadroid.websocket.AbstractCommunicator import AbstractCommunicator
from mapadroid.worker.WorkerState import WorkerState


class ClearGameDataEndpoint(AbstractControlEndpoint):
    """
    "/clear_game_data"
    """

    async def get(self):
        origin: Optional[str] = self.request.query.get("origin")
        useadb_raw: Optional[str] = self.request.query.get("adb")
        useadb: bool = True if useadb_raw is not None else False
        devicemapping: Optional[DeviceMappingsEntry] = await self._get_mapping_manager().get_devicemappings_of(origin)
        if not devicemapping:
            logger.warning("Device {} not found.", origin)
            return web.Response(text="Failed clearing game data.")
        if (useadb and
                await self._adb_connect.send_shell_command(devicemapping.device_settings.adbname, origin,
                                                           "pm clear com.nianticlabs.pokemongo")):
            pass
        else:
            temp_comm: Optional[AbstractCommunicator] = self._get_ws_server().get_origin_communicator(origin)
            if not temp_comm:
                return web.Response(text="Failed fetching connection to device.")
            await temp_comm.reset_app_data("com.nianticlabs.pokemongo")
            worker_state: Optional[WorkerState] = self._get_ws_server().get_worker_state(origin)
            if worker_state:
                worker_state.active_account = None
        raise web.HTTPFound(self._url_for("get_phonescreens"))
