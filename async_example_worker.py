# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-01 06:59:06
       $Rev: 17
"""

# BUILTIN modules
import time
import shutil
import asyncio
import datetime
from pathlib import Path
from typing import Union

# Third party modules
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Tools modules
from tools.configurator import config
from tools.async_utilities import delay
from tools.local_log_handler import logger
from tools.exceptions import error_message_of
from tools.async_cache_manager import AsyncCacheManager
from tools.async_file_searcher import AsyncFileSearcher
from tools.async_offline_buffer import AsyncOfflineBuffer
from tools.file_utilities import check_for_command, calculate_md5
from tools.async_rabbit_client import AsyncRabbitClient, RabbitParams

# Local program modules
from async_example_ini_core import AsyncExampleProgIni

# Constants
PREFIXES = ['Health', 'File', 'Error']
""" Send message prefixes. """
HEALTH_TEMPLATE = {'ExampleWorker.scheduler': False,
                   'AsyncFileSearcher.observer': False,
                   'AsyncRabbitClient.connection': False,
                   'ExampleWorker._message_broker': False,
                   'AsyncFileSearcher._message_broker': False}
""" Health report template for current program. """


# -----------------------------------------------------------------------------
#
class ExampleWorker:
    """
    This worker class demonstrates how to use resources like RabbitMQ and Watchdog
    asynchronously. It also demonstrates the usage of internal message brokers to
    control the work by using JSON messages as events. It also demonstrates the
    usage of scheduling reoccurring events.

    The following environment variable dependencies exist:
      - ENVIRONMENT

    The following secret dependencies exist:
      - mongoPwd

    The following jobs are scheduled:
      - *_schedule_dump_check()*: runs every five seconds.
      - *_schedule_state_pruning()*: runs at midnight every day.

    RabbitMQ is used for receiving and sending messages to external services.
    Messages that are to be sent to the RabbitMQ will be stored offline if
    the communication goes down and will be re-sent when the communication
    is re-established.

    Subscribe temporary for the following RabbitMQ message topic(s):
      - Health.Request.*

    Subscribe permanently for the following RabbitMQ message topic(s):
      - File.ReportRequest.<SERVER>

    Sends RabbitMQ messages with the following topic(s):
      - File.Report.<server>
      - File.Detected.<server>
      - Error.Message.<server>
      - Health.Response.<server>


    :ivar ini: Ini file configuration parameters.
    :type ini: `AsyncExampleProgIni`
    :ivar program: Current program name, used by logging and RabbitMQ.
    :type program: `str`
    :ivar detected_files: Keeping track of received files during the day.
    :type detected_files: `dict`
    :ivar health_report: Keep track of health status when a request arrives.
    :type health_report: `dict`
    :ivar work_queue: Used for transferring a message between interested parties.
    :type work_queue: `asyncio.Queue`
    :ivar scheduler: Handles dump checks and pruning of received_files cache.
    :type scheduler: ``apscheduler.schedulers.asyncio.AsyncIOScheduler``
    :ivar mq_buffer:
        Handle the storing and retrieving of messages when the external
        communication goes up and down.
    :type mq_buffer: `AsyncOfflineBuffer`
    :ivar mq_mgr: Handle messages being sent to or received from RabbitMQ.
    :type mq_mgr: `AsyncRabbitClient`
    :ivar cache_mgr:  Handles archiving and restoring cache data that needs to
        survive a program restart.
    :type cache_mgr: `AsyncCacheManager`
    :ivar searcher:  Reports new files detected in specified directories.
    :type searcher: `AsyncFileSearcher`
    """

    # ---------------------------------------------------------
    #
    def __init__(self, ini: AsyncExampleProgIni, program: str):
        """ The class constructor.

        :param ini: Ini file configuration parameters.
        :param program: Program name, used by logging and RabbitMQ.
        """

        # Local parameters.
        paths = [ini.in_path]
        params = RabbitParams(server=config.server, program=program)

        # Input parameters.
        self.ini = ini
        self.program = program

        # Unique parameters.
        self.detected_files = {}
        self.health_report = None

        # Initiate objects.
        self.work_queue = asyncio.Queue()
        self.scheduler = AsyncIOScheduler()
        self.mq_buffer = AsyncOfflineBuffer(ini.offline_path)
        self.mq_mgr = AsyncRabbitClient(config.rabbitUrl,
                                        params, self.work_queue)
        self.cache_mgr = AsyncCacheManager(self.ini.offline_path)
        self.searcher = AsyncFileSearcher(paths, self.work_queue)

    # ----------------------------------------------------------
    # Required in every program.
    #
    async def _report_error(self, error: Exception):
        """ Log error with context and failure data, then send it to RabbitMQ.

        :param error: Current exception.
        """
        logger.exception('Unhandled exception =>')

        # Trigger sending error message.
        msg = error_message_of(error, self.program, 'DUMP')
        await self.work_queue.put(msg)

    # ----------------------------------------------------------
    # required in every program.
    #
    async def _create_health_response(self):
        """ Create the program health response and send it. """

        data = {'msgType': 'HealthResponse',
                'server': config.server, 'timestamp': time.time(),
                'service': self.program, 'resources': self.health_report,
                'status': all(key for key in self.health_report.values())}
        await self.work_queue.put(data)

        # Reset parameter since the report have been sent.
        self.health_report = None

    # ----------------------------------------------------------
    # required in every program (but content is changed).
    #
    async def _process_health_request(self):
        """ Process HealthRequest message..

        Example msg data::

          {"msgType": "HealthRequest"}
        """

        self.health_report = HEALTH_TEMPLATE.copy()
        self.health_report |= {'ExampleWorker._message_broker': True,
                               'ExampleWorker.scheduler': self.scheduler.running}

        # Trigger status reports from used resources.
        msg = {'msgType': 'StatusRequest'}
        await self.mq_mgr.status_of()
        await self.searcher.notify(msg)

        # Give all reporting modules some time to submit
        # their health status before creating the report.
        _ = asyncio.create_task(delay(self._create_health_response(), 1))

    # ----------------------------------------------------------
    # required in every program.
    #
    async def _process_status_response(self, msg: dict):
        """ Process StatusResponse message..

        Example msg data::

          {"msgType": "StatusResponse", "resources": {...}}
        """

        for key, value in msg['resources'].items():
            self.health_report[key] = value

    # ----------------------------------------------------------
    # required in every program (but content is changed).
    #
    async def _process_linkup_message(self):
        """ Send pending offline messages and start subscription(s). """
        _ = asyncio.create_task(self._send_offline_messages())

        keys = ['Health.Request.*']
        _ = asyncio.create_task(
            self.mq_mgr.start_topic_subscription(keys, permanent=False))

        keys = [f'File.ReportRequest.{config.server}']
        _ = asyncio.create_task(self.mq_mgr.start_topic_subscription(keys))

    # ---------------------------------------------------------
    # Optional, needed when active cache(s) need to survive a
    # program restart.
    # Content in the items variable will change depending on
    # how many active cache(s) that need to be saved, and
    # their individual names.
    #
    async def _archive_active_cache(self, dump: bool = False):
        """ Archive active cache(s).

        When *dump=True* the archive filename uses a *"dump_"*
        prefix when saving the file.

        Note that you are currently limited to what YAML handles
        when it comes to what data types you can archive.

        :param dump: Dump status (default: False).
        """

        # ----------------------------------------------------------

        def name_of(attribute: Union[dict, list]) -> str:
            """ Return str name of supplied class attribute. """
            class_attributes = self.__dict__.items()
            return [name for name, value in class_attributes if value is attribute][0]

        # ----------------------------------------------------------

        # Initiate data for one or more active cache(s) that needs archiving.
        items = {name_of(item): item for item in [
            self.detected_files
        ] if item}

        await self.cache_mgr.archive_cache(items, dump)

    # ----------------------------------------------------------
    # Optional, needed when active cache(s) need to survive a
    # program restart.
    #
    async def _restore_active_cache(self):
        """ Restore active cache(s).

        Currently handled active cache types:
            - dict
            - list

        Note: this method will restore the saved state to the class
        attribute that was specified when it was archived.

        :raise ValueError: When archived cache name is not found.
        """
        items = await self.cache_mgr.restore_cache()

        for attribute_name, value in items.items():
            try:
                class_attribute = getattr(self, attribute_name)

                if isinstance(value, dict):
                    class_attribute.update(value)
                elif isinstance(value, list):
                    class_attribute.extend(value)

            # You have a name mismatch defined when archiving.
            except AttributeError:
                errmsg = (f'Archived cache name mismatch, attribute "self.'
                          f'{attribute_name}" is NOT defined in the worker '
                          f'class! Stop the app, rename the file on disk to'
                          f'match the new attribute name and restart the app.')
                raise ValueError(errmsg)

        # Remove the files when the restore was successful.
        await self.cache_mgr.clear_restored_cache()

    # ---------------------------------------------------------
    # Optional, needed when sending external messages.
    #
    async def _send_offline_messages(self):
        """ Send pending offline messages to the RabbitMQ server. """

        if not self.mq_buffer.is_empty:
            messages = await self.mq_buffer.retrieve()
            size = len(messages)

            for msg in messages:
                await self.work_queue.put(msg)

            logger.success('Restored {size} offline messages', size=size)

    # ---------------------------------------------------------
    # Optional, needed when sending external messages.
    #
    async def _handle_send_response(self, success: bool, msg: dict, topic: str):
        """ Handle send response, good or bad.

        If the communication is down, the message is stored offline.

        :param success: Message transmission status.
        :param msg: A msgType message.
        :param topic: A subscription topic.
        """

        if success:
            logger.success("Sent '{what}' message to MQ", what=topic)

        else:
            await self.mq_buffer.append(msg)
            logger.warning("Stored '{what}' message offline", what=topic)

    # ----------------------------------------------------------
    # Optional, needed when sending external messages.
    # (but content might change).
    #
    async def _send_message(self, msg: dict):
        """ Send a msgType message to RabbitMQ.

        The message topic is automatically created here with the help
        of the PREFIXES constant.

        Handle File and Error message types.

        :param msg: A msgType message.
        """

        msg_type = msg['msgType']
        key = next(prefix for prefix in PREFIXES if msg_type.startswith(prefix))
        topic = f"{'.'.join(msg_type.partition(key)[1:])}.{config.server}"
        result = await self.mq_mgr.publish_message(msg, topic)
        await self._handle_send_response(result, msg, topic)

    # ---------------------------------------------------------
    # Optional, needed when the worker class needs to know
    # when any of it's required INI parameters have changed,
    # or you want to show in the log that they have changed.
    #
    async def _process_new_params(self):
        """ Update affected resources with the changed Ini parameters. """

        if self.ini.changed_schedules:
            logger.info("Updating Ini 'schedules' configuration..")

        if self.ini.document_types:
            logger.info("Updating Ini 'document_types' configuration..")

    # ---------------------------------------------------------
    # Optional, needed when active cache content needs to be
    # viewed offline when the program is running.
    #
    async def _schedule_dump_check(self):
        """ Handle DUMP request in a separate task, if needed. """

        if check_for_command('dump'):
            _ = asyncio.create_task(self._archive_active_cache(dump=True))

    # ----------------------------------------------------------
    # Unique for this program.
    #
    async def _prune_cache_content(self):
        """
        Make a daily report of received_files that is older than today and
        remove the reported files from the cache.

        This method is normally called at midnight since the detected files
        cache should only contain files for the current day.

        When a program is started, we also have to make sure that all files
        older than today are reported and pruned from the current day file
        cache.
        """

        try:
            report = {}
            today = datetime.date.today()
            detected_files = self.detected_files.copy()

            for key, item in detected_files.items():
                file_age = today - item['when'].date()

                if file_age.days > 0:
                    item['when'] = item['when'].strftime("%Y-%m-%d %X")
                    report[key] = item
                    del self.detected_files[key]

            if report:
                logger.info('Daily report contains {cnt} pruned files', cnt=len(report))
                msg = {'msgType': 'FileReport', 'state': 'daily',
                       'server': config.server, 'data': report,
                       'service': self.program}
                await self.work_queue.put(msg)

        except BaseException as why:
            await self._report_error(why)

    # ----------------------------------------------------------
    # Unique for this program.
    #
    async def _process_file_found(self, msg: dict):
        """ Trigger a new workflow by sending a FileDetected message.

        If a duplicate file is detected, it's considered an error, so
        it's logged and the file is moved to the *error_path* directory.

        When it's a unique file, it's added to the detection cache, a
        *FileDetected* message is sent, and the file is moved to the
        *out_path* directory.

        Example msg data::

          {"msgType": "FileFound", "file": "D:/Prod/Pre/Incoming/AKFAIN12.DAT"}

        :param msg: A FileFound message.
        """

        try:
            infile = Path(msg['file'])
            crc = await calculate_md5(infile)
            duplicate = self.detected_files.get(crc)
            dest_path = (self.ini.error_path if duplicate else self.ini.out_path)

            if duplicate:
                errmsg = (f"Checksum {crc} already exists for received file "
                          f"{infile.name} => [{duplicate['when']}, "
                          f"{duplicate['name']}]")
                logger.error(errmsg)

            else:
                self.detected_files[crc] = {'name': str(infile),
                                            'size': infile.stat().st_size,
                                            'when': datetime.datetime.now()}

                data = {'msgType': 'FileDetected', 'file': str(infile)}
                await self.work_queue.put(data)

            dest_file = Path(dest_path, infile.name)
            shutil.move(infile, dest_file)

        except BaseException as why:
            await self._report_error(why)

    # ----------------------------------------------------------
    # Unique for this program.
    #
    async def _process_report_request(self):
        """ Process FileReportRequest message..

        Example msg data::

          {"msgType": "FileReportRequest"}
        """

        try:
            data = {'msgType': 'FileReport', 'server': config.server,
                    'service': self.program, 'state': 'requested',
                    'data': self.detected_files}
            await self.work_queue.put(data)

        except BaseException as why:
            await self._report_error(why)

    # ---------------------------------------------------------
    # Unique for this program.
    #
    async def _schedule_state_pruning(self):
        """ Start received_files pruning in a separate task, if needed. """

        if self.detected_files:
            _ = asyncio.create_task(self._prune_cache_content())

    # ---------------------------------------------------------
    # required in every program (but content changes).
    #
    async def _message_broker(self):
        """ Broker messages between interested parties using a queue.

        Received message types are:
            - Stop
            - LinkUp
            - FileFound
            - HealthRequest
            - StatusResponse
            - ChangedIniParams
            - FileReportRequest

        Sent message types are:
            - FileReport
            - FileDetected
            - ErrorMessage
            - HealthResponse
        """

        try:
            logger.trace('Starting main BROKER task...')

            while True:
                msg = await self.work_queue.get()

                # Exit the loop when the program termination is
                # triggered internally by a fatal exception.
                if msg['msgType'] == 'Stop':
                    break

                log = getattr(
                    logger, config.logSeverity.get(msg['msgType'], 'info')
                )
                log("Received a '{msgType}' message...", **msg)

                # Message types that are sent to RabbitMQ.
                if msg['msgType'] in (
                        'FileReport', 'FileDetected',
                        'ErrorMessage', 'HealthResponse'
                ):
                    await self._send_message(msg)

                # Send pending messages and start subscription(s).
                elif msg['msgType'] == 'LinkUp':
                    await self._process_linkup_message()

                # A new file is detected in the supervised context.
                elif msg['msgType'] == 'FileFound':
                    _ = asyncio.create_task(self._process_file_found(msg))

                # External message from RabbitMQ,
                elif msg['msgType'] == 'HealthRequest':
                    await self._process_health_request()

                # Status reports from active resources.
                elif msg['msgType'] == 'StatusResponse':
                    await self._process_status_response(msg)

                # The main program detected updated worker INI file parameters.
                elif msg['msgType'] == 'ChangedIniParams':
                    await self._process_new_params()

                # External message from RabbitMQ,
                elif msg['msgType'] == 'FileReportRequest':
                    await self._process_report_request()

            logger.trace('Stopped main BROKER task')

        except asyncio.CancelledError:
            logger.trace('Operator cancelled main BROKER task')

    # ----------------------------------------------------------
    # required in every program.
    #
    async def notify(self, msg: dict):
        """ Send msgType messages to the broker.

        :param msg: A msgType message.
        """
        self.work_queue.put_nowait(msg)

    # ---------------------------------------------------------
    # required in every program (but content is changed).
    #
    async def start(self):
        """ Start the used resources in a controlled way. """

        await self._restore_active_cache()

        # Start queue blocking tasks.
        _ = asyncio.create_task(self._message_broker())

        # Start tasks and threads.
        await self.mq_mgr.start()
        await self.searcher.start()

        # Make sure we start fresh every day (files older
        # than today are reported and removed from the cache).
        await self._schedule_state_pruning()

        # Start scheduling the dump command detection.
        self.scheduler.add_job(self._schedule_dump_check,
                               trigger='interval', seconds=5)

        # Start scheduling the received files pruning job
        # (it runs at midnight every day).
        self.scheduler.add_job(self._schedule_state_pruning,
                               trigger='cron', hour='0')

        self.scheduler.start()

    # ---------------------------------------------------------
    # required in every program (but content is changed).
    #
    async def stop(self):
        """ Stop the used resources in a controlled way.

        Note that this method can't be async
        """

        # Wait for tasks and threads to stop.
        await self.mq_mgr.stop()
        await self.searcher.stop()

        # Stop queue blocking tasks.
        stop = {'msgType': 'Stop'}
        await self.work_queue.put(stop)

        await self._archive_active_cache()
