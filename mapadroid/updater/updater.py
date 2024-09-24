import asyncio
import json
import os
import re
import time
from asyncio import CancelledError, Task
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional, Tuple, Union

import asyncio_rlock
import marshmallow_dataclass
from marshmallow import Schema

from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.mad_apk.abstract_apk_storage import AbstractAPKStorage
from mapadroid.mad_apk.utils import (is_newer_version, lookup_arch_enum,
                                     stream_package, supported_pogo_version)
from mapadroid.updater.Autocommand import Autocommand
from mapadroid.updater.GlobalJobLogAlgoType import GlobalJobLogAlgoType
from mapadroid.updater.GlobalJobLogEntry import GlobalJobLogEntry
from mapadroid.updater.JobReturn import JobReturn
from mapadroid.updater.JobStatus import JobStatus
from mapadroid.updater.JobType import JobType
from mapadroid.updater.SubJob import SubJob
from mapadroid.utils.apk_enums import APKArch, APKPackage, APKType
from mapadroid.utils.custom_types import MADPackages
from mapadroid.utils.CustomTypes import MessageTyping
from mapadroid.utils.DatetimeWrapper import DatetimeWrapper
from mapadroid.utils.logging import LoggerEnums, get_logger
from mapadroid.utils.madGlobals import MadGlobals
from mapadroid.websocket import AbstractCommunicator, WebsocketServer

logger = get_logger(LoggerEnums.utils)

SUCCESS_STATES = [JobStatus.SUCCESS, JobStatus.NOT_REQUIRED, JobStatus.NOT_SUPPORTED]


class DeviceUpdater(object):
    def __init__(self, websocket, db: DbWrapper, storage_obj: AbstractAPKStorage):
        self._websocket: WebsocketServer = websocket
        self._job_queue: asyncio.Queue[GlobalJobLogEntry] = asyncio.Queue()
        self._update_mutex = asyncio_rlock.RLock()
        self._db: DbWrapper = db
        self._log: Dict[str, GlobalJobLogEntry] = {}
        self._available_jobs: Dict[str, List[SubJob]] = {}
        self._running_jobs_per_origin: Dict[str, GlobalJobLogEntry] = {}
        self._storage_obj: AbstractAPKStorage = storage_obj
        self._sub_job_schema: Schema = marshmallow_dataclass.class_schema(SubJob)()
        self._global_job_log_entry_schema: Schema = marshmallow_dataclass.class_schema(GlobalJobLogEntry)()
        self._autocommand_schema: Schema = marshmallow_dataclass.class_schema(Autocommand)()
        self._stop_updater_threads: asyncio.Event = asyncio.Event()
        self.t_updater: List[Task] = []

    async def _load_log(self) -> None:
        log_file: str = "update_log.json"
        try:
            if os.path.exists(log_file):
                with open(log_file) as logfile:
                    loaded_log = json.load(logfile)
                if not isinstance(loaded_log, dict):
                    logger.warning("Unable to ")
                    os.remove(log_file)
                    return
                for issued_job_name, issued_job_raw in loaded_log.items():
                    if not isinstance(issued_job_raw, dict):
                        logger.warning("Ignoring entry {} of {} as it's not a dict", issued_job_name, log_file)
                    issued_job: GlobalJobLogEntry = self._global_job_log_entry_schema.load(issued_job_raw)
                    self._log[issued_job_name] = issued_job
        except json.decoder.JSONDecodeError:
            logger.error('Corrupted update_log.json file found. Deleting the file. Please check remaining disk space '
                         'or disk health.')
            os.remove(log_file)

    async def stop_updater(self):
        self._stop_updater_threads.set()
        for thread in self.t_updater:
            thread.cancel()

    async def start_updater(self):
        await self.stop_updater()
        await self._load_jobs()
        await self._load_log()
        await self._kill_old_jobs()
        await self._load_automatic_jobs()
        self._stop_updater_threads.clear()
        loop = asyncio.get_running_loop()
        for i in range(MadGlobals.application_args.job_thread_count):
            updater_task: Task = loop.create_task(self._process_update_queue(i))
            self.t_updater.append(updater_task)

    async def reload_jobs(self):
        await self._load_jobs()

    async def _load_jobs(self):
        async with self._update_mutex:
            self._available_jobs.clear()
            if os.path.exists('commands.json'):
                await self._load_jobs_from_file('commands.json')
            # load personal commands
            for command_file in Path("personal_commands").rglob("*.json"):
                try:
                    await self._load_jobs_from_file(command_file)
                except Exception as e:
                    logger.error('Cannot add job {} - Reason: {}', command_file, e)

            # Read all .apk files in the upload dir
            for apk_file in Path(MadGlobals.application_args.upload_path).rglob("*.apk"):
                created: int = int(os.path.getmtime(apk_file))

                self._available_jobs[os.path.basename(apk_file)] = [SubJob(TYPE=JobType.INSTALLATION,
                                                                           SYNTAX=os.path.basename(apk_file),
                                                                           FIELDNAME=str(created))]

    async def _load_jobs_from_file(self, file_path):
        with open(file_path) as cmdfile:
            commands_raw = json.loads(cmdfile.read())
        if not isinstance(commands_raw, dict):
            return
        for job_name, sub_job_list in commands_raw.items():
            if not isinstance(sub_job_list, list):
                logger.warning("{} contains invalid value: {}", file_path, sub_job_list)
                continue
            if job_name in self._available_jobs:
                logger.warning("Job {} already exists and is being overwritten by the file {}", job_name, file_path)
            sub_jobs: List[SubJob] = self._sub_job_schema.load(sub_job_list, many=True)
            self._available_jobs[job_name] = sub_jobs

    def get_available_jobs(self) -> Dict[str, List[SubJob]]:
        return self._available_jobs

    async def restart_job(self, job_id: str):
        async with self._update_mutex:
            if job_id not in self._log:
                return
            job_entry: GlobalJobLogEntry = self._log[job_id]
            job_entry.counter = 0
            job_entry.sub_job_index = 0
            if job_entry.processing_date is None or job_entry.processing_date < int(time.time()):
                job_entry.processing_date = None
            job_entry.last_status = JobStatus.PENDING

            await self._job_queue.put(job_entry)
            await self.__update_log(job_entry)

    async def _kill_old_jobs(self):
        logger.info("Checking for outdated jobs")
        async with self._update_mutex:
            for job_id in list(self._log.keys()):
                job_entry: GlobalJobLogEntry = self._log[job_id]
                if job_entry.last_status in {JobStatus.PENDING, JobStatus.STARTING, JobStatus.PROCESSING,
                                             JobStatus.NOT_CONNECTED, JobStatus.FUTURE, JobStatus.NOT_REQUIRED,
                                             JobStatus.FAILING}:
                    job_entry.last_status = JobStatus.CANCELLED
                elif job_entry.auto_command_settings is not None:
                    self._log.pop(job_id)
            await self.__write_log()

    async def __handle_job(self, job_item: GlobalJobLogEntry) -> bool:
        """

        Args:
            job_item:

        Returns: Boolean indicating if the job has been processed (True) or should be processed again (False)

        """
        if len(job_item.sub_jobs) <= job_item.sub_job_index:
            logger.success("Successfully executed job {} ({}, index {}) with {} subjobs",
                           job_item.id,
                           job_item.job_name,
                           job_item.sub_job_index,
                           len(job_item.sub_jobs))
            return True
        if job_item.last_status == JobStatus.CANCELLED:
            logger.info("Job {} of device {} has been cancelled and will not be processed anymore.",
                        job_item.job_name, job_item.origin)
            return True
        elif job_item.last_status == JobStatus.FAILED:
            # breakup job because last job in chain failed anyway
            logger.error("Breakup job {} on device {} - previous job in chain failed "
                         "(Job index: {}, Sub Jobs: {})", job_item.job_name, job_item.origin, job_item.sub_job_index,
                         job_item.sub_jobs)
            await self.send_webhook(job_item=job_item, status=JobReturn.TERMINATED)
            return True
        elif job_item.counter > 3:
            if job_item.last_status == JobStatus.FAILING:
                logger.error("Job {} for origin {} failed 3 times in row - aborting (index: {}, sub jobs: {})",
                             job_item.job_name, job_item.origin, job_item.sub_job_index,
                             job_item.sub_jobs)
                if job_item.auto_command_settings is not None and job_item.auto_command_settings.redo_on_error:
                    # Restart the job as the redo_on_error flag is set
                    await self.restart_job(job_id=job_item.id)
                job_item.last_status = JobStatus.FAILED
            elif job_item.last_status == JobStatus.NOT_CONNECTED \
                    and MadGlobals.application_args.job_restart_notconnect > 0:
                logger.error("Job {} for origin {} failed 3 times in row due to device not being connected - "
                             "requeing due to job_restart_notconnect being set to {}.",
                             job_item.job_name, job_item.origin,
                             MadGlobals.application_args.job_restart_notconnect)
                processtime = datetime.timestamp(
                    DatetimeWrapper.now() + timedelta(minutes=MadGlobals.application_args.job_restart_notconnect))
                job_item.processing_date = processtime
                await self.restart_job(job_id=job_item.id)
            else:
                logger.error("Job {} attempted to be executed for {} more than 3 times in row. Aborting.",
                             job_item.job_name, job_item.origin)
            # Do not process the same entry again
            return True

        if job_item.processing_date is None:
            # no processing date has been set, check if one should be set and set it accordingly if needed
            if job_item.auto_command_settings is not None:
                # Handle parameters for auto_commands
                # TODO: start_with_init should be run once
                minutes_until_next_exec: Optional[int] = self.get_job_algo_value(
                    job_item.auto_command_settings.algo_type,
                    job_item.auto_command_settings.algo_value)
                if minutes_until_next_exec is None:
                    logger.warning("Invalid job algo values found in job {}", job_item.job_name)
                    return True
                job_item.processing_date = int(time.time()) + 60 * minutes_until_next_exec

            # Next, evaluate the waittime of the SubJob to be run next
            # As an auto_command may also execute SubJobs with waittime, there's no else/elif here
            if len(job_item.sub_jobs) > job_item.sub_job_index \
                    and job_item.sub_jobs[job_item.sub_job_index].WAITTIME is not None:
                # The SubJob to be run next does indeed have a WAITTIME defined
                # if a processing_date has already been defined, add the needed desired waittime, else simply set it
                if job_item.processing_date is None:
                    job_item.processing_date = int(time.time())
                job_item.processing_date += 60 * job_item.sub_jobs[job_item.sub_job_index].WAITTIME

        # If a processing_date has been set, the job is not to be executed before that date. Thus, add it back to the
        # queue and basically repeat until it is to be executed.
        if job_item.processing_date is not None and job_item.processing_date > time.time():
            logger.debug3('Job {} on device {} - queued for (further) processing in the future.',
                          job_item.job_name, job_item.origin)
            job_item.last_status = JobStatus.FUTURE
            return False

        logger.info("Job for {} (SubJob Index: {}, SubJobs: {}) started (ID: {})",
                    job_item.origin,
                    job_item.sub_job_index,
                    job_item.sub_jobs,
                    job_item.id)
        job_item.last_status = JobStatus.PROCESSING
        job_item.processing_date = int(time.time())
        await self.__update_log(job_item)

        temp_comm: Optional[AbstractCommunicator] = self._websocket.get_origin_communicator(job_item.origin)
        if not temp_comm:
            logger.error("Cannot start job on device {} - SubJob index: {}, SubJobs: {} - Device not connected "
                         "(ID: {})",
                         job_item.origin,
                         job_item.sub_job_index,
                         job_item.sub_jobs,
                         job_item.id)
            job_item.last_status = JobStatus.NOT_CONNECTED
            job_item.counter += 1
            # Next attempt to execute in 60 seconds
            job_item.processing_date = int(time.time()) + 60
            return False

        job_item.last_status = JobStatus.STARTING
        await self.__update_log(job_item)
        try:
            if await self.__start_job_type(job_item, temp_comm):
                logger.info('SubJob of job {} executed successfully - Device {} - SubJob index: {} (SubJobs: {})',
                            job_item.job_name, job_item.origin,
                            job_item.sub_job_index,
                            job_item.sub_jobs)
                job_item.sub_job_index += 1
                job_item.last_status = JobStatus.SUCCESS
                job_item.counter = 0  # Reset the counter
            else:
                logger.error(
                    'SubJob of job {} could not be executed successfully - Device {} - SubJob index: {} (SubJobs: {})',
                    job_item.job_name, job_item.origin,
                    job_item.sub_job_index,
                    job_item.sub_jobs)
                job_item.last_status = JobStatus.FAILING
                return False
        except Exception as e:
            logger.error(
                'Job {} could not be executed successfully (fatal error) - Device {} - SubJob index: {} (SubJobs: {})',
                job_item.job_name, job_item.origin,
                job_item.sub_job_index,
                job_item.sub_jobs)
            job_item.last_status = JobStatus.INTERRUPTED
            return False
        finally:
            job_item.counter += 1

        if job_item.auto_command_settings is not None and job_item.auto_command_settings.redo:
            logger.info('Re-adding the automatic job {} of {} after having executed it in a non-failing state',
                        job_item.job_name,
                        job_item.origin)
            await self.restart_job(job_id=job_item.id)

        # TODO:
        # await self.send_webhook(job_id=job_id, status=jobstatus)

    async def _process_update_queue(self, threadnumber):
        logger.info("Starting device job processor thread No {}", threadnumber)
        await asyncio.sleep(10)
        while not self._stop_updater_threads.is_set():
            try:
                try:
                    item: Optional[GlobalJobLogEntry] = self._job_queue.get_nowait()
                except asyncio.queues.QueueEmpty:
                    await asyncio.sleep(1)
                    continue
                if item is None:
                    continue
                # Boolean to control the release of the running job on the device
                requeue: bool = False
                try:
                    async with self._update_mutex:
                        if item.id not in self._log:
                            continue

                        if item.origin in self._running_jobs_per_origin \
                                and item.id != self._running_jobs_per_origin[item.origin].id:
                            # Do not run multiple (different) jobs on the same device at once
                            await self._job_queue.put(item)
                            continue
                        self._running_jobs_per_origin[item.origin] = item

                    await self._websocket.set_job_activated(item.origin)
                    requeue = not await self.__handle_job(item)
                    if requeue:
                        await self._job_queue.put(item)
                except Exception as e:
                    logger.warning("Failed executing job")
                    logger.exception(e)
                finally:
                    await self._websocket.set_job_deactivated(item.origin)
                    await self.__update_log(item)
                    # While we requeue jobs of autocommands looping, these should not influence jobs to be run
                    #  at any other time
                    if not requeue or item.auto_command_settings is not None and item.last_status != JobStatus.FUTURE:
                        async with self._update_mutex:
                            self._running_jobs_per_origin.pop(item.origin)
                    self._job_queue.task_done()

            except (KeyboardInterrupt, CancelledError):
                logger.info("process_update_queue-{} received keyboard interrupt, stopping", threadnumber)
                break

            await asyncio.sleep(2)
        logger.info("Updater thread stopped")

    async def add_job(self, origin: str, job_name: str,
                      auto_command: Optional[Autocommand] = None) -> bool:
        if job_name not in self._available_jobs:
            logger.warning("Cannot add job '{}' as it is not loaded.", job_name)
            return False
        logger.info('Adding Job {} for Device {}', job_name, origin)

        jobs_to_run: List[SubJob] = self._available_jobs.get(job_name)
        job_id: str = f"{time.time()}_{origin}_{job_name}"

        new_entry: GlobalJobLogEntry = GlobalJobLogEntry(job_id, origin, job_name)
        new_entry.sub_jobs.extend(jobs_to_run)
        new_entry.auto_command_settings = auto_command

        await self._job_queue.put(new_entry)
        await self.__update_log(new_entry)
        return True

    async def __write_log(self):
        async with self._update_mutex:
            with open('update_log.json', 'w') as outfile:
                to_dump = {}
                for job_id, entry in self._log.items():
                    to_dump[job_id] = self._global_job_log_entry_schema.dump(entry, many=False)
                json.dump(to_dump, outfile, indent=4)

    async def delete_log_id(self, job_id: str):
        async with self._update_mutex:
            if job_id not in self._log:
                return True
            for origin, job_entry in self._running_jobs_per_origin.items():
                if job_entry.id == job_id:
                    return False
            self._log.pop(job_id)
            return True

    def get_log(self, including_auto_jobs=False) -> List[GlobalJobLogEntry]:
        if including_auto_jobs:
            return [self._log[x] for x in self._log if self._log[x].auto_command_settings is not None]
        return [self._log[x] for x in self._log if self._log[x].auto_command_settings is None]

    def get_log_serialized(self, including_auto_jobs=False) -> List[Dict]:
        plain_list = self.get_log(including_auto_jobs)
        return self._global_job_log_entry_schema.dump(plain_list, many=True)

    async def __start_job_type(self, job_item: GlobalJobLogEntry, communicator: AbstractCommunicator) -> bool:
        """

        Args:
            job_item:
            communicator:

        Returns: Boolean indicating whether the execution of the SubJob relevant was successful

        """
        sub_job_to_run: SubJob = job_item.sub_jobs[job_item.sub_job_index]
        try:
            if sub_job_to_run.TYPE == JobType.INSTALLATION:
                if str(sub_job_to_run.SYNTAX).lower().endswith(".apk"):
                    returning = await communicator.install_apk(300,
                                                               filepath=os.path.join(MadGlobals.application_args.upload_path,
                                                                                     sub_job_to_run.SYNTAX))
                elif str(sub_job_to_run.SYNTAX).lower().endswith(".zip"):
                    returning = await communicator.install_bundle(600,
                                                                  filepath=os.path.join(MadGlobals.application_args.upload_path,
                                                                                        sub_job_to_run.SYNTAX))
                else:
                    # unknown filetype
                    returning = False
                return returning if not 'RemoteGpsController'.lower() in str(sub_job_to_run.SYNTAX).lower() else True
            elif sub_job_to_run.TYPE == JobType.SMART_UPDATE:
                package_ver: Optional[str] = None
                package_raw: str = sub_job_to_run.SYNTAX
                version_job = "dumpsys package %s | grep versionName" % (package_raw,)
                architecture_job: Optional[MessageTyping] = await communicator.passthrough('getprop ro.product.cpu.abi')
                package_ver_job: Optional[MessageTyping] = await communicator.passthrough(version_job)
                # TODO: Verify the types of the above results
                try:
                    architecture_raw = re.search(r'\[(\S+)]', architecture_job).group(1)
                except AttributeError:
                    logger.warning('Unable to determine the architecture of the device')
                    return False
                try:
                    package_ver = re.search(r'versionName=([\d.]+)', package_ver_job).group(1)
                except AttributeError:
                    if package_ver_job and package_ver_job.split('\n')[0].strip() == 'OK':
                        logger.info('No information returned.  Assuming package is not installed')
                    else:
                        logger.warning('Unable to determine version for {}: {}', sub_job_to_run.SYNTAX,
                                       package_ver_job)
                        return False
                package = getattr(APKType, APKPackage(package_raw).name)
                architecture = lookup_arch_enum(architecture_raw)
                package_all: MADPackages = await self._storage_obj.get_current_package_info(package)
                if package_all is None:
                    logger.warning('No MAD APK for {} [{}]', package.name, architecture.name)
                    return False
                try:
                    mad_apk = package_all[architecture]
                except KeyError:
                    architecture = APKArch.noarch
                    mad_apk = package_all[architecture.noarch]

                if mad_apk.filename is None:
                    logger.warning('No MAD APK for {} [{}]', package, architecture.name)
                    return False
                # Validate it is supported
                if package == APKType.pogo:
                    if not await supported_pogo_version(architecture, mad_apk.version, self._storage_obj.token):
                        job_item.last_status = JobStatus.NOT_SUPPORTED
                        return True
                if not is_newer_version(mad_apk.version, package_ver):
                    logger.info('Both versions are the same.  No update required')
                    job_item.last_status = JobStatus.NOT_REQUIRED
                    return True
                else:
                    logger.info('Smart Update APK Installation for {} to {}',
                                package.name, job_item.origin)
                    apk_file = bytes()
                    async with self._db as session, session:
                        package_data: Optional[Tuple[AsyncGenerator,
                                                     str, str, str]] = await stream_package(session, self._storage_obj,
                                                                                            package, architecture)
                        if not package_data:
                            logger.warning("Unable to smart update due to failure to retrieve binary data of the APK "
                                           "requested.")
                            return True
                        gen_func, mimetype, filename, version = package_data
                        async for chunk in gen_func:
                            apk_file += chunk
                    if mad_apk.mimetype == 'application/zip':
                        returning = await communicator.install_bundle(300, data=apk_file)
                    else:
                        returning = await communicator.install_apk(300, data=apk_file)
                    return returning if not 'RemoteGpsController'.lower() in str(sub_job_to_run.SYNTAX).lower() \
                        else True
            elif sub_job_to_run.TYPE == JobType.REBOOT:
                return await communicator.reboot()
            elif sub_job_to_run.TYPE == JobType.RESTART:
                return await communicator.restart_app("com.nianticlabs.pokemongo")
            elif sub_job_to_run.TYPE == JobType.STOP:
                return await communicator.stop_app("com.nianticlabs.pokemongo")
            elif sub_job_to_run.TYPE == JobType.START:
                return await communicator.start_app("com.nianticlabs.pokemongo")
            elif sub_job_to_run.TYPE == JobType.PASSTHROUGH:
                returning = (await communicator.passthrough(sub_job_to_run.SYNTAX)) \
                    .replace('\r', '').replace('\n', '').replace('  ', '')
                job_item.returning = returning
                return not returning.startswith("KO:")
            return False
        except Exception as e:
            logger.error('Error while getting response from device - Reason: {}', e)
        return False

    async def delete_log(self, only_success=False):
        """

        Args:
            only_success: Only delete jobs with a status considered successful

        Returns:

        """
        if only_success:
            for job_id in list(self._log.keys()):
                job_entry: GlobalJobLogEntry = self._log[job_id]
                if job_entry.last_status in SUCCESS_STATES and (job_entry.auto_command_settings is None or
                                                                not job_entry.auto_command_settings.redo):
                    self._log.pop(job_id)
        else:
            for job_id in list(self._log.keys()):
                await self.delete_log_id(job_id)

    async def send_webhook(self, job_item: GlobalJobLogEntry, status: JobReturn):
        if not job_item.auto_command_settings:
            # Only send webhooks for auto commands
            return

        try:
            if status.name not in MadGlobals.application_args.job_dt_send_type.split(
                    '|') or not MadGlobals.application_args.job_dt_wh:
                return

            from discord_webhook import DiscordEmbed, DiscordWebhook

            # TODO: Async
            _webhook = DiscordWebhook(url=MadGlobals.application_args.job_dt_wh_url)

            logger.info("Send discord status for device {} (Job: {})", job_item.origin, job_item.job_name)

            embed = DiscordEmbed(title='MAD Job Status', description='Automatic Job processed', color=242424)
            embed.set_author(name='MADBOT')
            embed.add_embed_field(name='Origin', value=job_item.origin)
            embed.add_embed_field(name='Jobname', value=job_item.job_name)
            embed.add_embed_field(name='Retuning', value=job_item.processing_date)
            embed.add_embed_field(name='Status', value=status.name)
            embed.add_embed_field(name='Next run',
                                  value=str(DatetimeWrapper.fromtimestamp(
                                      job_item.processing_date) if job_item.processing_date is not None else "-"))
            _webhook.add_embed(embed)
            _webhook.execute()
            embed = None
        except Exception as e:
            logger.error('Cannot send discord webhook for origin {} - Job {} - Reason: {}',
                         job_item.origin, job_item.job_name, e)

    async def _load_automatic_jobs(self):
        autocommandfile = os.path.join(MadGlobals.application_args.file_path, 'autocommands.json')
        if not os.path.exists(autocommandfile) or not os.path.isfile(autocommandfile):
            logger.info('No autocommand file available at {}', autocommandfile)
            return

        with open(autocommandfile) as cmdfile:
            autocommands = json.loads(cmdfile.read())

        autocommands_loaded: List[Autocommand] = self._autocommand_schema.load(autocommands, many=True)
        if not autocommands_loaded:
            logger.info('No autocommands loaded from {}', autocommandfile)
            return

        logger.info('Found {} autojobs - add them', len(autocommands_loaded))
        for autocommand in autocommands_loaded:
            origins: List[str] = autocommand.origins.split('|')
            for origin in origins:
                await self.add_job(origin=origin, job_name=autocommand.job, auto_command=autocommand)

    def get_job_algo_value(self, algo_type: GlobalJobLogAlgoType, algo_value: Union[str, int]) -> Optional[int]:
        """
        Returns the amount of minutes to the next event
        Args:
            algo_type:
            algo_value:

        Returns:

        """
        if algo_type == GlobalJobLogAlgoType.LOOP and isinstance(algo_value, int):
            return algo_value
        elif algo_type == GlobalJobLogAlgoType.DAILY and isinstance(algo_value, str):
            algo_value_split: List[str] = algo_value.split(':')
            if len(algo_value_split) != 2:
                logger.warning("Job Algo Type Daily requires a hh:mm formatted value.")
                return None
            tm = DatetimeWrapper.now().replace(
                hour=int(algo_value_split[0]), minute=int(algo_value_split[1]), second=0, microsecond=0)

            return int((tm - DatetimeWrapper.now()).seconds / 60)
        else:
            logger.warning("Invalid combination of job algo type ({}) and value ({})",
                           algo_type, algo_value)
            return None

    async def __update_log(self, entry: Optional[GlobalJobLogEntry]):
        async with self._update_mutex:
            if entry is not None and entry.id not in self._log:
                self._log[entry.id] = entry
            await self.__write_log()
