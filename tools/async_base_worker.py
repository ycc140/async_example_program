# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-09 18:52:05
       $Rev: 24
"""

# BUILTIN modules
import time
import asyncio
from abc import abstractmethod
from typing import Optional

# Tools modules
from tools import exceptions
from tools.configurator import config
from tools.local_log_handler import logger
from tools.async_offline_buffer import AsyncOfflineBuffer
from tools.async_ini_file_parser import AsyncIniFileParser
from tools.async_rabbit_client import AsyncRabbitClient, RabbitParams


# -----------------------------------------------------------------------------
#
class AsyncBaseWorker:
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


    :ivar ini: Ini file configuration parameters.
    :type ini: `AsyncIniFileParser`
    :ivar program: Current program name, used by logging and RabbitMQ.
    :type program: `str`
    :ivar send_prefixes: Current programs send message prefixes.
    :type send_prefixes: L{list}
    :ivar health_report: Keeps track of health status when a request arrives.
    :type health_report: `dict`
    :ivar work_queue: Used for transferring a message between interested parties.
    :type work_queue: `asyncio.Queue`
    :ivar mq_mgr: Handle messages being sent to or received from RabbitMQ.
    :type mq_mgr: `AsyncRabbitClient`
    :ivar mq_buffer:
        Handle the storing and retrieving of messages when the external
        communication goes up and down.
    :type mq_buffer: `AsyncOfflineBuffer`
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

        # Initiate objects.
        self.work_queue = asyncio.Queue()
        self.mq_mgr = AsyncRabbitClient(
            config.rabbitUrl, params, self.work_queue)
        self.mq_buffer = AsyncOfflineBuffer(ini.offline_path)

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
