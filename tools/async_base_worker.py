# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-12 19:37:19
       $Rev: 33
"""

# BUILTIN modules
import time
import asyncio
from abc import abstractmethod
from typing import Optional, Union

# Tools modules
from tools import exceptions
from tools.configurator import config
from tools.local_log_handler import logger
from tools.async_offline_buffer import AsyncOfflineBuffer
from tools.async_ini_file_parser import AsyncIniFileParser
from tools.async_rabbit_client import AsyncRabbitClient, RabbitParams
from tools.async_state_offline_manager import AsyncStateOfflineManager


# -----------------------------------------------------------------------------
#
class AsyncBaseWorker:
    """
    This generic base worker class is intended to be used as a building block
    through inheritance when you are creating a server worker.

    Any methods that you need to change, you override it using polymorphism.


    :ivar ini: Ini file configuration parameters.
    :type ini: `AsyncIniFileParser`
    :ivar program: Current program name, used by logging and RabbitMQ.
    :type program: `str`
    :ivar send_prefixes: Current programs send message prefixes.
    :type send_prefixes: L{list}
    :ivar health_report: Keeps track of health status when a request arrives.
    :type health_report: `dict`
    :ivar states_to_archive:  Contains state(s) that needs archiving.
    :type states_to_archive: `list`
    :ivar work_queue: Used for transferring a message between interested parties.
    :type work_queue: `asyncio.Queue`
    :ivar mq_mgr: Handle messages being sent to or received from RabbitMQ.
    :type mq_mgr: `AsyncRabbitClient`
    :ivar mq_buffer:
        Handle the storing and retrieving of messages when the external
        communication goes up and down.
    :type mq_buffer: `AsyncOfflineBuffer`
    :ivar state_mgr:  Handles archiving and restoring state data that needs to
        survive a program restart.
    :type state_mgr: `AsyncStateOfflineManager`
    """

    # ---------------------------------------------------------
    #
    def __init__(self, ini: AsyncIniFileParser, program: str, send_prefixes: list):
        """ The class constructor.

        :param ini: Ini file configuration parameters.
        :param program: Program name, used by logging and RabbitMQ.
        :param send_prefixes: Current programs send prefixes.
        """

        # Local parameters.
        params = RabbitParams(server=config.server, program=program)

        # Input parameters.
        self.ini = ini
        self.program = program
        self.send_prefixes = send_prefixes

        # Unique parameters.
        self.health_report = None
        self.states_to_archive = []

        # Initiate objects.
        self.work_queue = asyncio.Queue()
        self.mq_mgr = AsyncRabbitClient(
            config.rabbitUrl, params, self.work_queue)
        self.mq_buffer = AsyncOfflineBuffer(ini.offline_path)
        self.state_mgr = AsyncStateOfflineManager(self.ini.offline_path)

    # ----------------------------------------------------------
    #
    async def _report_error(self, error: Exception, state: str = 'DUMP',
                            extra: Optional[str] = None):
        """ Log error with context and failure data, then send it to RabbitMQ.

        :param error: Current exception.
        :param state: Error message state.
        :param extra: Additional error text.
        """
        errmsg = exceptions.error_text_of(error, extra=extra)

        if state == 'DUMP':
            traceback = True
            logger.opt(exception=error).error(f'{errmsg} => ')

        else:
            traceback = False
            logger.error('{err}', err=errmsg)

        # Trigger sending error message.
        msg = exceptions.error_message_of(error, program=self.program,
                                          state=state, include_traceback=traceback)
        self.work_queue.put_nowait(msg)

    # ---------------------------------------------------------
    # Optional, needed when active state(s) need to survive a
    # program restart.
    # Content in the items variable will change depending on
    # how many active state(s) that need to be saved, and
    # their individual names.
    #
    async def _archive_active_state(self, dump: bool = False):
        """ Archive active state(s).

        When *dump=True* the archive filename uses a *"dump_"*
        prefix when saving the file.

        Note that you are currently limited to what YAML handles
        when it comes to what data types you can archive.

        :param dump: Dump status (default: False).
        """

        # ----------------------------------------------------------

        def name_of(attr_value: Union[dict, list]) -> str:
            """ Return str name of supplied class attribute value. """
            class_attr = self.__dict__.items()
            return [attr for attr, val in class_attr if val is attr_value][0]

        # ----------------------------------------------------------

        # Create a data structure for one or more active state(s) that
        # needs archiving (has content).
        items = {name_of(item): item for item in self.states_to_archive if item}

        await self.state_mgr.archive_state(items, dump)

    # ----------------------------------------------------------
    # Optional, needed when active state(s) need to survive a
    # program restart.
    #
    async def _restore_active_state(self):
        """ Restore active state(s).

        Currently handled active state types:
            - dict
            - list

        Note: this method will restore the saved state to the class
        attribute that was specified when it was archived.

        :raise ValueError: When archived state name is not found.
        """
        items = await self.state_mgr.restore_state()

        for attribute_name, value in items.items():
            try:
                class_attribute = getattr(self, attribute_name)

                if isinstance(value, dict):
                    class_attribute.update(value)
                elif isinstance(value, list):
                    class_attribute.extend(value)

            # You have a name mismatch defined when archiving.
            except AttributeError:
                errmsg = (f'Archived state name mismatch, attribute "self.'
                          f'{attribute_name}" is NOT defined in the worker '
                          f'class! Stop the app, rename the file on disk to'
                          f'match the new attribute name and restart the app.')
                raise ValueError(errmsg)

        # Remove the files when the restore was successful.
        await self.state_mgr.clear_restored_state()

    # ----------------------------------------------------------
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
    #
    @abstractmethod
    async def _process_health_request(self):
        """ Process HealthRequest message.

        Abstract method that needs to be overridden.
        """
        ...

    # ----------------------------------------------------------
    #
    async def _process_status_response(self, msg: dict):
        """ Process StatusResponse message..

        Example msg data:

        .. python::
          {"msgType": "StatusResponse", "resources": {...}}
        """

        for key, value in msg['resources'].items():
            self.health_report[key] = value

    # ----------------------------------------------------------
    #
    async def _process_linkup_message(self):
        """ Send pending offline messages and start subscription(s). """
        _ = asyncio.create_task(self._send_offline_messages())

        keys = ['Health.Request']
        _ = asyncio.create_task(
            self.mq_mgr.start_topic_subscription(keys, permanent=False))

    # ---------------------------------------------------------
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
    #
    # noinspection StructuralWrap
    async def _send_message(self, msg: dict):
        """ Send a msgType message to RabbitMQ.

        The message topic is automatically created here with the help
        of the PREFIXES constant.

        You only have to change *msg_type* when you are using more than
        the two standardized topic patterns.

        An example of that might look like this:

        .. Python::
            # Send message prefixes.
            PREFIXES = ['Health', 'Error', 'Email', 'Phoenix', 'File']

            if msg['msgType'] == 'PhoenixRegisterFile':
                msg_type = 'PhoenixRegister.File'

            elif msg['msgType'] in config.watchTopics:
                msg_type = f"{msg['msgType']}.{self.program}"

            else:
                msg_type = msg['msgType']

        :param msg: A msgType message.
        """
        prefixes = self.send_prefixes
        msg_type = (f"{msg['msgType']}.{self.program}"
                    if msg['msgType'] in config.watchTopics else msg['msgType'])
        key = next(prefix for prefix in prefixes if msg_type.startswith(prefix))
        topic = f"{'.'.join(msg_type.partition(key)[1:])}.{config.server}"
        result = await self.mq_mgr.publish_message(msg, topic)
        await self._handle_send_response(result, msg, topic)

    # ---------------------------------------------------------
    #
    async def _message_broker(self):
        """ Broker messages between interested parties using a queue.

        This is the minimum setup for the main message broker. If
        you need more routes, you have to override this method in
        your own code (and copy the content as a base for your work).

        Handled received message types are:
            - Stop
            - LinkUp
            - HealthRequest
            - StatusResponse

        Handled sent message types are:
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
                        'HealthResponse', 'ErrorMessage'
                ):
                    await self._send_message(msg)

                # Send pending messages and start subscription(s).
                elif msg['msgType'] == 'LinkUp':
                    await self._process_linkup_message()

                # External message from RabbitMQ,
                elif msg['msgType'] == 'HealthRequest':
                    await self._process_health_request()

                # Status reports from active resources.
                elif msg['msgType'] == 'StatusResponse':
                    await self._process_status_response(msg)

            logger.trace('Stopped main BROKER task')

        except asyncio.CancelledError:
            logger.trace('Operator cancelled main BROKER task')

    # ----------------------------------------------------------
    #
    async def notify(self, msg: dict):
        """ Send msgType messages to the broker.

        :param msg: A msgType message.
        """
        self.work_queue.put_nowait(msg)

    # ---------------------------------------------------------
    #
    async def start(self):
        """ Start the used resources in a controlled way. """

        # Only activate when used.
        if self.states_to_archive:
            await self._restore_active_state()

        # Start queue blocking tasks.
        _ = asyncio.create_task(self._message_broker())

        # Start tasks and threads.
        await self.mq_mgr.start()

    # ---------------------------------------------------------
    #
    async def stop(self):
        """ Stop the used resources in a controlled way. """

        # Wait for tasks and threads to stop.
        await self.mq_mgr.stop()

        # Stop queue blocking tasks.
        stop = {'msgType': 'Stop'}
        await self.work_queue.put(stop)

        # Only activate when used.
        if self.states_to_archive:
            await self._archive_active_state()
