from mapadroid.account_handler import AbstractAccountHandler
from mapadroid.data_handler.mitm_data.AbstractMitmMapper import \
    AbstractMitmMapper
from mapadroid.data_handler.stats.AbstractStatsHandler import \
    AbstractStatsHandler
from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.db.model import SettingsWalkerarea
from mapadroid.mapping_manager.MappingManager import MappingManager
from mapadroid.ocr.pogoWindows import PogoWindows
from mapadroid.ocr.screenPath import WordToScreenMatching
from mapadroid.utils.madGlobals import QuestLayer
from mapadroid.websocket.AbstractCommunicator import AbstractCommunicator
from mapadroid.worker.strategy.quest.QuestStrategy import QuestStrategy
from mapadroid.worker.WorkerState import WorkerState


class ARQuestLayerStrategy(QuestStrategy):
    def __init__(self, area_id: int, communicator: AbstractCommunicator,
                 mapping_manager: MappingManager,
                 db_wrapper: DbWrapper, word_to_screen_matching: WordToScreenMatching,
                 pogo_windows_handler: PogoWindows,
                 walker: SettingsWalkerarea,
                 worker_state: WorkerState,
                 mitm_mapper: AbstractMitmMapper,
                 stats_handler: AbstractStatsHandler,
                 account_handler: AbstractAccountHandler):
        super().__init__(area_id, communicator, mapping_manager, db_wrapper, word_to_screen_matching,
                         pogo_windows_handler, walker, worker_state,
                         mitm_mapper, stats_handler, QuestLayer.AR, account_handler=account_handler)

    async def pre_work_loop_layer_preparation(self) -> None:
        vps_delay: int = await self._get_vps_delay()
        current_layer = None
        try:
            current_layer = await self.get_current_layer_of_worker()
        except ValueError as e:
            pass
        if current_layer != QuestLayer.AR:
            # Trigger the deletion of quests
            # await self._clear_quests(vps_delay, openmenu=True)
            pass
        # Nothing else to do given the deletion of quests leaves us on the AR layer
        self._ready_for_scan.set()

    async def _check_layer(self) -> None:
        try:
            if await self.get_current_layer_of_worker() == QuestLayer.AR:
                self._ready_for_scan.set()
            else:
                vps_delay: int = await self._get_vps_delay()
                # await self._clear_quests(vps_delay, openmenu=True)
        except ValueError as e:
            pass
