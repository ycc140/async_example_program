# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-09-28 20:42:35
       $Rev: 1
"""

# BUILTIN modules
import time
import json
import shutil
import asyncio
from typing import Union
from pathlib import Path

# Third party modules
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Tools modules
from tools.configurator import config
from tools.local_log_handler import logger
from tools.exceptions import error_message_of
from tools.file_utilities import check_for_command
from tools.async_cache_manager import AsyncCacheManager
from tools.async_file_searcher import AsyncFileSearcher
from tools.async_offline_buffer import AsyncOfflineBuffer
from tools.async_rabbit_client import AsyncRabbitClient, RabbitParams

# Local program modules
from async_example_ini_core import AsyncExampleProgIni

# Constants
PREFIXES = ['File', 'Error']
""" Send message prefixes. """


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

    Subscribe for the following RabbitMQ message topic(s):
      - 'File.ReportRequest.<SERVER>'

    Sends RabbitMQ messages with the following topic(s):
      - 'File.Report.<server>'
      - 'File.Detected.<server>'
      - 'Error.Message.<server>'


    :ivar ini: Ini file configuration parameters.
    :type ini: `AsyncExampleProgIni`
    :ivar program: Current program name, used by logging and RabbitMQ.
    :type program: `str`
    :ivar received_files: Keeping track of received files during the day.
    :type received_files: `dict`
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
        self.received_files = {}

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
            self.received_files
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

        :raise RuntimeError: When archived cache name is not found.
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
                raise RuntimeError(errmsg)

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
            await asyncio.create_task(self._archive_active_cache(dump=True))

    # ----------------------------------------------------------
    # Unique for this program.
    #
    async def _prune_state_content(self):
        """ Report received_files content, then clear it for today's content.

        This method needs to be called at midnight since *self.received_files*
        should only contain files for the current day.
        """
        try:
            data = {'msgType': 'FileReport',
                    'service': self.program, 'state': 'daily', 'server': config.server,
                    'data': json.dumps(self.received_files, ensure_ascii=False)}
            await self.work_queue.put(data)

            self.received_files.clear()

        except BaseException as why:
            await self._report_error(why)

    # ----------------------------------------------------------
    # Unique for this program.
    #
    async def _process_file_found(self, msg: dict):
        """ Trigger a new workflow by sending a FileDetected message.

        Example msg data::

          {"msgType": "FileFound", "type": "detected",
           "file": "D:/Prod/Pre/ExampleProgram/In/AAAA_invoices.txt"}

        :param msg: A FileFound message.
        """

        try:
            infile = Path(msg['file'])
            outfile = Path(self.ini.out_path) / infile.name
            shutil.move(infile, outfile)

            data = {'msgType': 'FileDetected', 'file': str(outfile)}
            await self.work_queue.put(data)

            # Keep track of received files.
            data = {'size': outfile.stat().st_size,
                    'when': time.strftime("%Y-%m-%d %X")}
            self.received_files.setdefault(outfile.name, []).append(data)

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
            data = {'msgType': 'FileReport',
                    'service': self.program, 'state': 'requested',
                    'data': json.dumps(self.received_files), 'server': config.server}
            await self.work_queue.put(data)

        except BaseException as why:
            await self._report_error(why)

    # ---------------------------------------------------------
    # Unique for this program.
    #
    async def _schedule_state_pruning(self):
        """ Start received_files pruning in a separate task, if needed. """

        if self.received_files:
            await asyncio.create_task(self._prune_state_content())

    # ---------------------------------------------------------
    # required in every program (but content changes).
    #
    async def _message_broker(self):
        """ Broker messages between interested parties using a queue.

        Received message types are:
            - Stop
            - LinkUp
            - FileFound
            - ChangedIniParams
            - FileReportRequest

        Sent message types are:
            - FileReport
            - FileDetected
            - ErrorMessage
        """

        try:
            logger.trace('Starting BROKER task...')

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
                        'FileReport', 'FileDetected', 'ErrorMessage'
                ):
                    await self._send_message(msg)

                elif msg['msgType'] == 'LinkUp':
                    await asyncio.create_task(self._send_offline_messages())

                    keys = [f'File.ReportRequest.{config.server}']
                    await asyncio.create_task(
                        self.mq_mgr.start_topic_subscription(keys))

                # A new file is detected in the supervised context.
                elif msg['msgType'] == 'FileFound':
                    await self._process_file_found(msg)

                # The main program detected updated worker INI file parameters.
                elif msg['msgType'] == 'ChangedIniParams':
                    await self._process_new_params()

                # External message from RabbitMQ,
                elif msg['msgType'] == 'FileReportRequest':
                    await self._process_report_request()

            logger.trace('Stopped BROKER task')

        except asyncio.CancelledError:
            logger.trace('Operator cancelled BROKER task')

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
        _ = [asyncio.create_task(self._message_broker())]

        # Start tasks and threads.
        await self.mq_mgr.start()
        await self.searcher.start()

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
