import asyncio
import time
from typing import List, Optional

import orjson
from aiohttp import web
from loguru import logger

import mapadroid.mitm_receiver.protos.Rpc_pb2 as pogoprotos
from mapadroid.db.helper.SettingsDeviceHelper import SettingsDeviceHelper
from mapadroid.db.helper.TrsVisitedHelper import TrsVisitedHelper
from mapadroid.db.model import SettingsDevice
from mapadroid.mitm_receiver.endpoints.AbstractMitmReceiverRootEndpoint import \
    AbstractMitmReceiverRootEndpoint
from mapadroid.mitm_receiver.protos.ProtoHelper import ProtoHelper
from mapadroid.utils.collections import Location
from mapadroid.utils.DatetimeWrapper import DatetimeWrapper
from mapadroid.utils.ProtoIdentifier import ProtoIdentifier


class ReceiveProtosEndpoint(AbstractMitmReceiverRootEndpoint):
    """
    "/"
    """

    async def _iter(self):
        # TODO: VisitorPattern for extra auth checks...
        with logger.contextualize(identifier=self._get_request_address(), name="receive_protos"):
            await self._check_origin_header()
            return await super()._iter()

    # TODO: Auth
    async def post(self):
        raw_data = await self.request.read()
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None, self.__process_data_to_json, raw_data)
        del raw_data
        origin = self.request.headers.get("origin")
        with logger.contextualize(identifier=origin, name="receive_protos"):
            logger.debug2("Receiving proto")
            await self._get_mapping_manager().increment_login_tracking_by_origin(origin)
            logger.debug4("Proto data received {}", data)
            if isinstance(data, list):
                # list of protos... we hope so at least....
                logger.debug2("Receiving list of protos")
                for proto in data:
                    await self.__handle_proto_data_dict(origin, proto)
            elif isinstance(data, dict):
                logger.debug2("Receiving single proto")
                # single proto, parse it...
                await self.__handle_proto_data_dict(origin, data)

            # del data
            return web.Response(status=200)

    def __process_data_to_json(self, raw_data):
        raw_text = raw_data.decode('utf8')
        data = orjson.loads(raw_text)
        del raw_text
        return data

    async def __handle_proto_data_dict(self, origin: str, data: dict) -> None:
        proto_type = data.get("type", None)
        if proto_type is None or proto_type == 0:
            logger.warning("Could not read method ID. Stopping processing of proto")
            return
        timestamp: int = data.get("timestamp", int(time.time()))
        if self._get_mad_args().mitm_ignore_pre_boot is True and timestamp < self._get_mitmreceiver_startup_time():
            return

        if proto_type not in (106, 102, 101, 104, 4, 156, 145, 1405):
            # trash protos - ignoring
            return

        location_of_data: Location = Location(data.get("lat", 0.0), data.get("lng", 0.0))
        if (location_of_data.lat > 90 or location_of_data.lat < -90 or
                location_of_data.lng > 180 or location_of_data.lng < -180):
            location_of_data: Location = Location(0.0, 0.0)
        time_received: int = int(time.time())

        quests_held: Optional[List[int]] = data.get("quests_held", None)
        await self._get_mitm_mapper().set_quests_held(origin, quests_held)

        if not data.get("raw", False):
            # Legacy json processing...
            logger.warning("JSON formatted processing is deprecated")
            return
        # Parsing raw data should be done within the data processor rather than the endpoint except for time
        # relevant information as the update_latest directive for example?
        # TODO: Offload to threads or does this have too much overhead?
        # TODO: dataclass for data passed in
        decoded_raw_proto: bytes = ProtoHelper.decode(data["payload"])
        data["payload"] = decoded_raw_proto

        if proto_type == ProtoIdentifier.GMO.value:
            # TODO: Offload transformation
            gmo: pogoprotos.GetMapObjectsOutProto = ProtoHelper.parse(ProtoIdentifier.GMO, decoded_raw_proto)
            if gmo.status != 1 or not gmo.map_cell:
                logger.debug("Ignoring apparently empty GMO or unsuccessful status {}", gmo.status)
                return
        elif proto_type == ProtoIdentifier.FORT_SEARCH.value:
            logger.debug("Checking fort search proto type 101")
            fort_search: pogoprotos.FortSearchOutProto = ProtoHelper.parse(ProtoIdentifier.FORT_SEARCH,
                                                                           decoded_raw_proto)
            if fort_search.result == 2:
                location_of_data: Location = Location(data.get("lat", 0.0), data.get("lng", 0.0))
                # Fort search out of range, abort
                logger.debug("Received out of range fort search for {}. Location of data: {}",
                             fort_search.get("fort_id", "unknown_id"), location_of_data)
                return

            await self._handle_fort_search_proto(origin, fort_search, location_of_data, timestamp)
        elif proto_type == ProtoIdentifier.ENCOUNTER.value:
            # TODO: Offload transformation
            encounter: pogoprotos.EncounterOutProto = ProtoHelper.parse(ProtoIdentifier.ENCOUNTER, decoded_raw_proto)
            if encounter.status != 1:
                logger.warning("Encounter with status {} being ignored", encounter.status)
                return
        elif proto_type == ProtoIdentifier.GET_ROUTES.value:
            get_routes: pogoprotos.GetRoutesOutProto = ProtoHelper.parse(ProtoIdentifier.GET_ROUTES, decoded_raw_proto)
            if not get_routes.route_map_cell:
                logger.info("No routes in payload to be processed")
                return

        await self._get_mitm_mapper().update_latest(origin, timestamp_received_raw=timestamp,
                                                    timestamp_received_receiver=time_received,
                                                    key=str(proto_type),
                                                    value=data["payload"],
                                                    location=location_of_data)

        logger.debug2("Placing data received to data_queue")
        await self._add_to_queue((timestamp, data, origin))

    async def _handle_fort_search_proto(self, origin: str, quest_proto: pogoprotos.FortSearchOutProto,
                                        location_of_data: Location,
                                        timestamp: int) -> None:
        instance_id = self._get_db_wrapper().get_instance_id()
        logger.debug("Checking fort search of {} of instance {}", origin, instance_id)
        device: Optional[SettingsDevice] = await SettingsDeviceHelper.get_by_origin(self._session,
                                                                                    self._get_db_wrapper().get_instance_id(),
                                                                                    origin)

        if not device:
            logger.debug("Device not found")
        await self._get_account_handler().set_last_softban_action(
            device.device_id, location_of_action=location_of_data,
            time_of_action=DatetimeWrapper.fromtimestamp(timestamp))
        self._commit_trigger = True
        if not quest_proto.fort_id:
            logger.debug("No fort id in fort search")
            return
        username: Optional[str] = await self._get_account_handler().get_assigned_username(device_id=device.device_id)
        if username:
            await TrsVisitedHelper.mark_visited(self._session, username, quest_proto.fort_id)
        else:
            logger.warning("Unable to retrieve username last assigned to {} to mark stop as visited", origin)
        # TODO: Stop doing anything after the above marking as visited given nothing happens below
        if not quest_proto.challenge_quest:
            logger.debug("No challenge quest in fort search")
            return
        # TODO: This chaining of property access probably is not safe to call like this...
        rewards = quest_proto.challenge_quest.quest.quest_rewards
        if not rewards:
            logger.debug("No quest rewards in fort search")
            return
