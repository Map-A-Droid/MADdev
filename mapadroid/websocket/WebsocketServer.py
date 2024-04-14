import asyncio
import logging
import random as rand
import time
from asyncio import CancelledError
from typing import Dict, List, Optional, Tuple

import websockets

from mapadroid.account_handler.AbstractAccountHandler import \
    AbstractAccountHandler
from mapadroid.data_handler.mitm_data.AbstractMitmMapper import \
    AbstractMitmMapper
from mapadroid.data_handler.stats.AbstractStatsHandler import \
    AbstractStatsHandler
from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.db.helper.SettingsDeviceHelper import SettingsDeviceHelper
from mapadroid.db.helper.SettingsPogoauthHelper import SettingsPogoauthHelper
from mapadroid.db.model import (AuthLevel, SettingsAuth, SettingsDevice,
                                SettingsPogoauth)
from mapadroid.mapping_manager.MappingManager import MappingManager
from mapadroid.mapping_manager.MappingManagerDevicemappingKey import \
    MappingManagerDevicemappingKey
from mapadroid.ocr.pogoWindows import PogoWindows
from mapadroid.utils.authHelper import check_auth, get_auths_for_levl
from mapadroid.utils.CustomTypes import MessageTyping
from mapadroid.utils.logging import InterceptHandler, LoggerEnums, get_logger
from mapadroid.utils.madGlobals import WebsocketAbortRegistrationException
from mapadroid.utils.pogoevent import PogoEvent
from mapadroid.websocket.AbstractCommunicator import AbstractCommunicator
from mapadroid.websocket.communicator import Communicator
from mapadroid.websocket.WebsocketConnectedClientEntry import \
    WebsocketConnectedClientEntry
from mapadroid.worker.strategy.AbstractWorkerStrategy import \
    AbstractWorkerStrategy
from mapadroid.worker.strategy.StrategyFactory import StrategyFactory
from mapadroid.worker.Worker import Worker
from mapadroid.worker.WorkerState import WorkerState

CONNECTING_TIMEOUT = 300

logging.getLogger('websockets.server').setLevel(logging.DEBUG)
logging.getLogger('websockets.protocol').setLevel(logging.DEBUG)
logging.getLogger('websockets.server').addHandler(InterceptHandler(log_section=LoggerEnums.websocket))
logging.getLogger('websockets.protocol').addHandler(InterceptHandler(log_section=LoggerEnums.websocket))
logger = get_logger(LoggerEnums.websocket)


class WebsocketServer(object):
    def __init__(self, args, mitm_mapper: AbstractMitmMapper, stats_handler: AbstractStatsHandler,
                 db_wrapper: DbWrapper, mapping_manager: MappingManager,
                 pogo_window_manager: PogoWindows, event,
                 account_handler: AbstractAccountHandler,
                 enable_configmode: bool = False,
                 ):
        self.__args = args
        self.__db_wrapper: DbWrapper = db_wrapper
        self.__mapping_manager: MappingManager = mapping_manager
        self.__pogo_window_manager: PogoWindows = pogo_window_manager
        self.__mitm_mapper: AbstractMitmMapper = mitm_mapper
        self.__stats_handler: AbstractStatsHandler = stats_handler
        self.__enable_configmode: bool = enable_configmode

        # Event to signal that the server is to be stopped. Used to not accept new connections for example
        self.__stop_server: asyncio.Event = asyncio.Event()

        # Dict keeping currently running connections and workers for management
        # Do think twice before plainly accessing, there's locks to be used
        self.__current_users: Dict[str, WebsocketConnectedClientEntry] = {}
        self.__current_users_mutex: Optional[asyncio.Lock] = None
        self.__users_connecting: Dict[str, int] = {}
        self.__users_connecting_mutex: Optional[asyncio.Lock] = None

        self.__strategy_factory: StrategyFactory = StrategyFactory(self.__args, self.__mapping_manager,
                                                                   self.__mitm_mapper, self.__stats_handler,
                                                                   self.__db_wrapper, self.__pogo_window_manager,
                                                                   event,
                                                                   account_handler=account_handler)
        self.__pogo_event: PogoEvent = event

        # asyncio loop for the entire server
        self.__loop: Optional[asyncio.AbstractEventLoop] = None
        self.__server_task: Optional[websockets.WebSocketServer] = None

    async def __setup_first_loop(self):
        logger.debug("Device mappings: {}", await self.__mapping_manager.get_all_devicemappings())

        self.__current_users_mutex: asyncio.Lock = asyncio.Lock()
        self.__users_connecting_mutex: asyncio.Lock = asyncio.Lock()

    async def start_server(self) -> None:
        logger.info("Starting websocket-server...")

        self.__loop = asyncio.get_running_loop()
        await self.__setup_first_loop()
        # the type-check here is sorta wrong, not entirely sure why
        # noinspection PyTypeChecker
        self.__server_task: websockets.WebSocketServer = await websockets.serve(self.__connection_handler,
                                                                                self.__args.ws_ip,
                                                                                int(self.__args.ws_port),
                                                                                max_size=2 ** 25,
                                                                                close_timeout=10)

    async def __close_all_connections_and_signal_stop(self):
        logger.info("Signaling all workers to stop")
        async with self.__current_users_mutex:
            for worker_entry in self.__current_users.values():
                if worker_entry.worker_instance:
                    await worker_entry.worker_instance.stop_worker()
                await self.__close_websocket_client_connection(worker_entry.origin,
                                                               worker_entry.websocket_client_connection)
        logger.info("Done signalling all workers to stop")

    async def stop_server(self) -> None:
        logger.info("Trying to stop websocket server")
        self.__stop_server.set()
        # wait for connecting users to empty
        while len(self.__users_connecting) > 0:
            logger.info("Shutdown of websocket waiting for connecting devices")
            await asyncio.sleep(1)

        await self.__close_all_connections_and_signal_stop()
        self.__server_task.close()
        await self.__server_task.wait_closed()
        logger.info("Stopped websocket server")

    async def __connection_handler(self, websocket_client_connection: websockets.WebSocketClientProtocol,
                                   path: str) -> None:
        """
        In case a new connection is being established, this method is called.
        Consequently, we are trying to create a worker that should live for as long as the connection is alive.
        Args:
            websocket_client_connection:
            path:

        Returns:

        """
        if self.__stop_server.is_set():
            return
        origin: Optional[str]
        success: Optional[bool]
        (origin, success) = await self.__authenticate_connection(websocket_client_connection)
        if not success:
            # failed auth, stop connection
            return
        with (logger.contextualize(identifier=origin, name="websocket")):
            logger.info("New connection from {}", websocket_client_connection.remote_address)
            async with self.__users_connecting_mutex:
                if (origin in self.__users_connecting
                        and self.__users_connecting[origin] + CONNECTING_TIMEOUT > int(time.time())):
                    logger.info("Client is already connecting")
                    return
                else:
                    self.__users_connecting[origin] = int(time.time())
            entry: Optional[WebsocketConnectedClientEntry] = None
            try:
                entry = await self.__setup_client_entry(origin=origin,
                                                        websocket_client_connection=websocket_client_connection)
            except WebsocketAbortRegistrationException:
                await asyncio.sleep(rand.uniform(3, 15))
                return
            except Exception as e:
                logger.opt(exception=True).error("Other unhandled exception during registration: {}", e)
                return
            finally:
                await self.__remove_from_users_connecting(origin)

            if entry:
                try:
                    await self.__client_message_receiver(entry)
                except CancelledError:
                    logger.info("Connection to {} has been cancelled", origin)
                # also check if thread is already running to not start it again. If it is not alive,
                # we need to create it..
                finally:
                    logger.info("Awaiting unregister")
                    # TODO: Only remove after some time to keep a worker state

            logger.info("Done with connection ({})", websocket_client_connection.remote_address)

    async def __setup_client_entry(self, origin: str, websocket_client_connection: websockets.WebSocketClientProtocol) \
            -> Optional[WebsocketConnectedClientEntry]:
        device: Optional[SettingsDevice] = None
        device_paused: bool = self.__enable_configmode
        device_id: int = -1
        if not self.__enable_configmode:
            logger.debug("Fetching device settings")
            async with self.__db_wrapper as session, session:
                device = await SettingsDeviceHelper.get_by_origin(session, self.__db_wrapper.get_instance_id(),
                                                                  origin)
            if not device:
                logger.warning("Device {} unknown", origin)
                return None
            else:
                device_id = device.device_id
                logger.debug("Checking if device is active")
                if not await self.__mapping_manager.is_device_active(device.device_id):
                    logger.warning('Origin is currently paused. Unpause through MADmin to begin working')
                    device_paused = True
        entry: Optional[WebsocketConnectedClientEntry] = None
        async with self.__current_users_mutex:
            logger.debug("Checking if an entry is already present")
            entry = self.__current_users.get(origin, None)

            # First check if an entry is present, worker running etc...
            if entry and entry.websocket_client_connection:
                await self.__handle_existing_connection(entry, origin)
                entry.websocket_client_connection = websocket_client_connection
            elif not entry:
                async with self.__db_wrapper as session, session:
                    current_auth: Optional[SettingsPogoauth] = await SettingsPogoauthHelper \
                        .get_assigned_to_device(session, device_id)
                    if current_auth:
                        session.expunge(current_auth)
                # Just create a new entry...
                worker_state: WorkerState = WorkerState(origin=origin,
                                                        device_id=device_id,
                                                        stop_worker_event=asyncio.Event(),
                                                        pogo_windows=self.__pogo_window_manager,
                                                        active_event=self.__pogo_event,
                                                        current_auth=current_auth)
                entry = WebsocketConnectedClientEntry(origin=origin,
                                                      websocket_client_connection=websocket_client_connection,
                                                      worker_instance=None,
                                                      worker_state=worker_state)
                self.__current_users[origin] = entry

        # No connection known or already at a point where we can continue creating worker
        # -> we can just create a new task
        if not await self.__add_worker_and_thread_to_entry(entry, origin, use_configmode=device_paused):
            logger.warning("Failed to start worker for {}", origin)
            raise WebsocketAbortRegistrationException("Failed starting worker")
        else:
            logger.info("Worker added/started successfully for {}", origin)
            return entry

    async def __remove_from_users_connecting(self, origin):
        async with self.__users_connecting_mutex:
            if origin in self.__users_connecting:
                self.__users_connecting.pop(origin)

    async def __remove_from_current_users(self, origin):
        async with self.__current_users_mutex:
            if origin in self.__current_users:
                del self.__current_users[origin]

    async def __handle_existing_connection(self, entry, origin):
        # An entry is already present, check the connection
        while not entry.websocket_client_connection.open and not entry.websocket_client_connection.closed:
            # Old connection is closing or opening, wait a moment
            logger.info("Old connection of {} closing/opening, waiting a moment", origin)
            await asyncio.sleep(1)
        if entry.websocket_client_connection.open:
            logger.error("Old connection open while a new one is attempted to be established, "
                         "aborting handling of connection")
            raise WebsocketAbortRegistrationException("Old connection still open")
        elif entry.websocket_client_connection.closed:
            # Old connection is closed, i.e. no active connection present...
            logger.info("Old connection of {} closed.",
                        origin)
        else:
            # Old connection neither open or closed - either closing or opening...
            # Should have been handled by the while loop above...
            raise WebsocketAbortRegistrationException("Old connection in some intermediate state")

    async def __add_worker_and_thread_to_entry(self, entry: WebsocketConnectedClientEntry,
                                               origin, use_configmode: bool = None) -> bool:
        logger.info("Trying to create new worker for {}.", origin)
        communicator: AbstractCommunicator = Communicator(
            entry, origin, None, self.__args.websocket_command_timeout)
        use_configmode: bool = use_configmode if use_configmode is not None else self.__enable_configmode

        scan_strategy: Optional[AbstractWorkerStrategy] = await self.__strategy_factory \
            .get_strategy_using_settings(origin, use_configmode, communicator=communicator,
                                         worker_state=entry.worker_state)
        if not scan_strategy:
            logger.warning("No strategy could be determined")
            return False
        if not entry.worker_instance:
            logger.info("Creating new worker")
            entry.worker_instance = Worker(worker_state=entry.worker_state,
                                           mapping_manager=self.__mapping_manager,
                                           db_wrapper=self.__db_wrapper,
                                           scan_strategy=scan_strategy,
                                           strategy_factory=self.__strategy_factory)
        else:
            logger.info("Updating strategy")
            await entry.worker_instance.set_scan_strategy(scan_strategy)
        communicator.worker_instance_ref = entry.worker_instance
        async with self.__current_users_mutex:
            return await entry.worker_instance.start_worker() is not None

    async def __authenticate_connection(self, websocket_client_connection: websockets.WebSocketClientProtocol) \
            -> Tuple[Optional[str], bool]:
        """
        :param websocket_client_connection:
        :return: origin (string) if the auth and everything else checks out, else None to signal abort
        """
        try:
            origin = str(
                websocket_client_connection.request_headers.get_all("Origin")[0])
        except IndexError:
            logger.warning("Client from {} tried to connect without Origin header",
                           websocket_client_connection.remote_address)
            return None, False
        logger.info("Client registering")
        if self.__mapping_manager is None:
            logger.warning("No configuration has been defined. Please define in MADmin and click "
                           "'APPLY SETTINGS'")
            return origin, False
        elif origin not in (await self.__mapping_manager.get_all_devicemappings()).keys():
            async with self.__db_wrapper as session, session:
                device = await SettingsDeviceHelper.get_by_origin(session, self.__db_wrapper.get_instance_id(), origin)
            if device:
                logger.warning("Device is created but not loaded. Click 'APPLY SETTINGS' in MADmin to Update")
            else:
                logger.warning("Register attempt of unknown origin ({}). Please create the device in MADmin and"
                               " click 'APPLY SETTINGS'", origin)
            return origin, False
        if not self.__args.insecure_auth:
            async with self.__db_wrapper as session, session:
                valid_auths: Dict[str, SettingsAuth] = await get_auths_for_levl(self.__db_wrapper,
                                                                                AuthLevel.MITM_DATA)
                auth_base64 = None
                if valid_auths:
                    try:
                        auth_base64 = str(
                            websocket_client_connection.request_headers.get_all("Authorization")[0])
                    except IndexError:
                        logger.warning("Client tried to connect without auth header")
                        return origin, False
                if not check_auth(logger, auth_base64, valid_auths):
                    return origin, False
        return origin, True

    async def __client_message_receiver(self, client_entry: WebsocketConnectedClientEntry) -> None:
        if client_entry is None:
            return
        connection: websockets.WebSocketClientProtocol = client_entry.websocket_client_connection
        logger.info("Consumer handler starting")
        while connection.open:
            message = None
            try:
                message = await connection.recv()
                # message = await asyncio.wait_for(connection.recv(), timeout=4.0)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.02)
            except websockets.ConnectionClosed as cc:
                logger.warning("Connection was closed, stopping receiver. Exception: {}", repr(cc))
                return

            if message is not None:
                await self.__on_message(client_entry, message)
        logger.warning("Connection closed in __client_message_receiver")

    async def _stop_worker(self, origin: str) -> None:
        entry: Optional[WebsocketConnectedClientEntry] = self.__current_users.get(origin, None)
        if entry and entry.worker_instance:
            await entry.worker_instance.stop_worker()

    @staticmethod
    async def __on_message(client_entry: WebsocketConnectedClientEntry, message: MessageTyping) -> None:
        response: Optional[MessageTyping] = None
        try:
            if isinstance(message, str):
                logger.debug("Receiving message: {}", message.strip())
                splitup = message.split(";", 1)
                message_id = int(splitup[0])
                response = splitup[1]
            else:
                logger.debug("Received binary values.")
                message_id = int.from_bytes(message[:4], byteorder='big', signed=False)
                response = message[4:]
        except ValueError as e:
            logger.warning("Failed reading message ID of message received for {} ({})", client_entry.origin, repr(e))
            return
        await client_entry.set_message_response(message_id, response)

    @staticmethod
    async def __close_websocket_client_connection(origin_of_worker: str,
                                                  websocket_client_connection: websockets.WebSocketClientProtocol) \
            -> None:
        logger.info('Closing connection of {}', origin_of_worker)
        await websocket_client_connection.close()
        logger.info("Connection of {} closed", origin_of_worker)

    async def get_connected_origins(self) -> List[str]:
        async with self.__current_users_mutex:
            origins_connected: List[str] = []
            for origin, entry in self.__current_users.items():
                if entry.websocket_client_connection.open:
                    origins_connected.append(origin)
            return origins_connected

    async def get_reg_origins(self) -> List[str]:
        return await self.get_connected_origins()

    def get_origin_communicator(self, origin: str) -> Optional[AbstractCommunicator]:
        # TODO: this should probably lock?
        entry: Optional[WebsocketConnectedClientEntry] = self.__current_users.get(origin, None)
        return (entry.worker_instance.get_communicator()
                if entry is not None and entry.worker_instance is not None
                else None)

    def set_geofix_sleeptime_worker(self, origin: str, sleeptime: int) -> bool:
        entry: Optional[WebsocketConnectedClientEntry] = self.__current_users.get(origin, None)
        return (entry.worker_instance.set_geofix_sleeptime(sleeptime)
                if entry is not None and entry.worker_instance is not None
                else False)

    async def set_job_activated(self, origin) -> None:
        await self.__mapping_manager.set_devicesetting_value_of(origin, MappingManagerDevicemappingKey.JOB_ACTIVE, True)

    async def set_job_deactivated(self, origin) -> None:
        await self.__mapping_manager.set_devicesetting_value_of(origin, MappingManagerDevicemappingKey.JOB_ACTIVE,
                                                                False)

    async def force_cancel_worker(self, origin) -> None:
        async with self.__current_users_mutex:
            entry: Optional[WebsocketConnectedClientEntry] = self.__current_users.get(origin, None)
            if not entry or not entry.worker_instance:
                return
            # Cancelling the scan task should result in the scan strategy to be updated
            await entry.worker_instance.cancel_scan()

    def get_worker_state(self, origin: str) -> Optional[WorkerState]:
        connection_entry: Optional[WebsocketConnectedClientEntry] = self.__current_users.get(origin, None)
        return connection_entry.worker_state if connection_entry else None
