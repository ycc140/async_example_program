# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2024-03-05 20:04:52
       $Rev: 42
"""

# BUILTIN modules
import shutil
import asyncio
import datetime
from pathlib import Path

# Third party modules
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Tools modules
from tools.configurator import config
from tools.async_utilities import delay
from tools.local_log_handler import logger
from tools.async_base_worker import AsyncBaseWorker
from tools.async_file_searcher import AsyncFileSearcher
from tools.file_utilities import check_for_command, calculate_md5

# Local program modules
from async_example_ini_core import AsyncExampleProgIni

# Constants
PREFIXES = ['Health', 'File', 'Error']
""" Send message prefixes. """
HEALTH_TEMPLATE = {'AsyncFileSearcher.observer': False,
                   'AsyncRabbitClient.connection': False,
                   'AsyncExampleWorker.scheduler': False,
                   'AsyncFileSearcher._message_broker': False,
                   'AsyncExampleWorker._message_broker': False}
""" Health report template for current program. """


# -----------------------------------------------------------------------------
#
class AsyncExampleWorker(AsyncBaseWorker):
    """
    This worker class demonstrates how to use resources like RabbitMQ and Watchdog
    asynchronously. It also demonstrates the usage of internal message brokers to
    control the work by using JSON messages as events. It also demonstrates the
    usage of scheduling reoccurring events.

    The following environment variable dependencies exist:
      - ENVIRONMENT
      - HOSTNAME (on Linux servers only - set by OS)
      - COMPUTERNAME (on Windows servers only - set by OS)

    The following secret dependencies exist:
      - mongo_pwd

    The following jobs are scheduled:
      - *_schedule_dump_check()*: runs every five seconds.
      - *_schedule_state_pruning()*: runs at midnight every day.

    RabbitMQ is used for receiving and sending messages to external services.
    Messages that are to be sent to the RabbitMQ will be stored offline if
    the communication goes down and will be re-sent when the communication
    is re-established.

    Subscribe temporary for the following RabbitMQ message topic(s):
      - Health.Request

    Subscribe permanently for the following RabbitMQ message topic(s):
      - File.ReportRequest.<SERVER>

    Sends RabbitMQ messages with the following topic(s):
      - File.Report.<server>
      - File.Detected.<server>
      - Error.Message.AsyncExampleProgram.<server>
      - Health.Response.AsyncExampleProgram.<server>


    :ivar health_report: Keeping track of health status when a request arrives.
    :type health_report: `dict`
    :ivar detected_files: Keeping track of received files during the day.
    :type detected_files: `dict`
    :ivar scheduler: Handles dump checks and pruning of received_files state.
    :type scheduler: ``apscheduler.schedulers.asyncio.AsyncIOScheduler``
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
        super().__init__(ini, program, PREFIXES)

        # Unique parameters.
        self.detected_files = {}

        # Initiate objects.
        self.scheduler = AsyncIOScheduler()
        self.searcher = AsyncFileSearcher(
            list(self.ini.document_types.keys()), self.work_queue)

    # ----------------------------------------------------------
    # required in every program (but content changes).
    #
    async def _process_health_request(self):
        """ Process HealthRequest message..

        Example msg data:

        .. python::
          {"msgType": "HealthRequest"}
        """
        name = self.__class__.__name__
        self.health_report = HEALTH_TEMPLATE.copy()
        self.health_report |= {f'{name}._message_broker': True,
                               f'{name}.scheduler': self.scheduler.running}

        # Trigger status reports from used resources.
        msg = {'msgType': 'StatusRequest'}
        await self.searcher.notify(msg)
        await self.mq_mgr.status_of()

        # Give all reporting modules some time to submit
        # their health status before creating the report.
        _ = asyncio.create_task(delay(self._create_health_response(), 1))

    # ---------------------------------------------------------
    # Optional, needed when the worker class needs to know
    # when any of it's required INI parameters have changed,
    # or you want to show in the log that they have changed.
    #
    async def _process_new_params(self):
        """ Update affected resources with the changed Ini parameters. """

        if self.ini.changed_document_types:
            logger.info("Updating changed Ini 'document_types' configuration...")
            msg = {'msgType': 'UpdateSearchPaths',
                   'data': list(self.ini.document_types.keys())}
            await self.searcher.notify(msg)

    # ----------------------------------------------------------
    # Unique for this program.
    #
    async def _prune_state_content(self):
        """
        Make a daily report of received_files that is older than today and
        remove the reported files from the state.

        This method is normally called at midnight since the detected files
        state should only contain files for the current day.

        When a program is started, we also have to make sure that all files
        older than today are reported and pruned from the current-day file
        state.
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
                logger.info('Daily report contains {cnt} '
                            'pruned files', cnt=len(report))
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

        When it's a unique file, it's added to the detection state, a
        *FileDetected* message is sent, and the file is moved to the
        *out_path* directory.

        Example msg data:

        .. Python::
            {"msgType": "FileFound",
             "file": "D:\\prod\\kundin\\cust3\\DDDD.231008.txt"}

        :param msg: A FileFound message.
        """

        try:
            infile = Path(msg['file'])
            key = infile.parent.as_posix()

            # Verify that it's a file that we are interested in.
            if infile.name[:4] not in self.ini.document_types[key]:
                dest_file = Path(self.ini.error_path, infile.name)
                shutil.move(infile, dest_file)
                logger.warning('Unknown file type {name} is move to {where}',
                               name=infile.name, where=self.ini.error_path)
                return

            crc = await calculate_md5(infile)
            duplicate = self.detected_files.get(crc)
            dest_path = (self.ini.error_path
                         if duplicate else self.ini.out_path)

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
        """ Process FileReportRequest message.

        Handled sent message types are:
            - FileReport
        """

        try:
            data = {'msgType': 'FileReport', 'server': config.server,
                    'service': self.program, 'state': 'requested',
                    'data': self.detected_files.copy()}
            await self.work_queue.put(data)

        except BaseException as why:
            await self._report_error(why)

    # ---------------------------------------------------------
    # Optional, needed when active state content needs to be
    # viewed offline when the program is running.
    #
    async def _schedule_dump_check(self):
        """ Handle DUMP request in a separate task, if needed. """

        if check_for_command('dump'):
            _ = asyncio.create_task(self._archive_active_state(dump=True))

    # ---------------------------------------------------------
    # Unique for this program.
    #
    async def _schedule_state_pruning(self):
        """ Start received_files pruning in a separate task, if needed. """

        if self.detected_files:
            _ = asyncio.create_task(self._prune_state_content())

    # ----------------------------------------------------------
    # Optional, needed when you have unique subscribe topics.
    #
    async def _process_linkup_message(self):
        """ Send pending offline messages and start subscription(s). """
        await super()._process_linkup_message()

        keys = [f'File.ReportRequest.{config.server}']
        _ = asyncio.create_task(self.mq_mgr.start_topic_subscription(keys))

    # ---------------------------------------------------------
    # Optional, needed when you have unique messages that needs routing.
    # Copy the method from the base class and add your extra routes.
    #
    async def _message_broker(self):
        """ Broker messages between interested parties using a queue.

        Handled received message types are:
            - Stop
            - LinkUp
            - FileFound
            - HealthRequest
            - StatusResponse
            - ChangedIniParams
            - FileReportRequest

        Handled sent message types are:
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
                        'HealthResponse', 'ErrorMessage',
                        'FileDetected', 'FileReport',
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

    # ---------------------------------------------------------
    # Optional, needed when you are using extra resources.
    # Remember to call the method in the base class as well.
    #
    async def start(self):
        """ Start the used resources in a controlled way.

        You need the statement, appending the state(s) that needs to
        survive a program restart to be placed first in the method
        (the *self.states_to_archive* class attribute is defined in
        the inherited base class).

        It could look something like this::

            self.states_to_archive.append(self.detected_files)
        """
        self.states_to_archive.append(self.detected_files)

        await super().start()

        # Start tasks and threads.
        await self.searcher.start()

        # Make sure we start fresh every day (files older
        # than today are reported and removed from the state).
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
    # Optional, needed when you are using extra resources.
    # Remember to call the method in the base class as well.
    #
    async def stop(self):
        """ Stop the used resources in a controlled way.

        Note that this method can't be async
        """
        await super().stop()

        # Wait for tasks and threads to stop.
        await self.searcher.stop()

        if self.scheduler.running:
            self.scheduler.shutdown()
