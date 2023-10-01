# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-01 06:15:18
       $Rev: 14
"""

# BUILTIN modules
import json
import asyncio
from dataclasses import dataclass
from typing import Optional, Union, Any

# Third party modules
from pamqp.commands import Basic
from aiormq.abc import DeliveredMessage
from aio_pika.exceptions import AMQPConnectionError, ConnectionClosed
from aio_pika import connect_robust, Message, DeliveryMode, exceptions
from aio_pika.abc import (AbstractIncomingMessage, AbstractRobustConnection)

# Local program modules
from tools.configurator import config
from tools.local_log_handler import logger

# Constants
CONTENT_TYPE = 'application/json'
""" Defines JSON as the content type. """


# -----------------------------------------------------------------------------
#
@dataclass(frozen=True)
class RabbitParams:
    """ Representation of RabbitMQ configuration parameters.


    :ivar server: Local server, used for topic queue name.
    :ivar program: Program name, used for topic queue name.
    """
    server: str
    program: str


# -----------------------------------------------------------------------------
#
class AsyncRabbitClient:
    """ This class implements RabbitMQ Publish and Subscribe async handling.

    The RabbitMQ queue mechanism is used so that we can take advantage of
    good horizontal message scaling when needed.

    Note that you only need to specify the rabbit_url parameter when you
    are sending messages, params and resp_queue are used for subscriptions.

    Functionality is implemented for:
      - Sending work queue messages (on default exchange).
      - Sending topic messages (on 'topic_routing' exchange).
      - Consuming work queue messages using a permanent queue (on default exchange).
      - Consuming topic messages using a temporary queue (on 'topic_routing' exchange).
      - Consuming topic messages using a permanent queue (on 'topic_routing' exchange).

    Sends the following messages:
      - {"msgType": "LinkUp", "host": <RabbitMQ-server>}
      - {"msgType": "LinkDown", "host": "<RabbitMQ-server>", "why": "<exception text>"}
      - {"msgType": <any>, ...}


    :ivar conf:
        RabbitParams object instance for RabbitMQ config parameters.
    :type conf: `RabbitParams` or `None`
    :ivar rabbit_url: RabbitMQ's connection URL.
    :type rabbit_url: `str`
    :ivar resp_queue: Destination queue for consumed messages.
    :type resp_queue: `asyncio.Queue` or `None`
    :ivar host: RabbitMQ servername.
    :type host: `str`
    :ivar channel: RabbitMQ's connection channel object instance.
    :type channel: ``aio_pika.abc.AbstractChannel``
    :ivar connection: RabbitMQ's connection object instance.
    :type connection: ``aio_pika.abc.AbstractRobustConnection``
    :ivar link_down_reported: LinkDown reported status.
    :type link_down_reported: `bool`
    """

    # ---------------------------------------------------------
    #
    def __init__(self, rabbit_url: str,
                 params: Optional[RabbitParams] = None,
                 resp_queue: Optional[asyncio.Queue] = None):
        """ The class initializer.

        :param rabbit_url: RabbitMQ's connection URL.
        :param params: Configuration parameters.
        :param resp_queue: Destination queue for consumed messages.
        """

        # Input parameters.
        self.conf = params
        self.rabbit_url = rabbit_url
        self.resp_queue = resp_queue

        # Unique parameters.
        self.host = None
        self.channel = None
        self.connection = None
        self.link_down_reported = False

    # ---------------------------------------------------------
    #
    def _on_connection_closed(self, _: Any, error: AMQPConnectionError):
        """ Handle unexpectedly closed connection events.

        :param error: AMQP connection error instance.
        """

        # Notify the user of this class that the RabbitMQ communication is down (only once).
        if not self.link_down_reported:
            msg = {"msgType": "LinkDown", "host": self.host}

            if isinstance(error, ConnectionClosed):
                msg["why"] = error.strerror

            self.resp_queue.put_nowait(msg)
            self.link_down_reported = True

    # ---------------------------------------------------------
    #
    async def _on_connection_reconnected(self, connection: AbstractRobustConnection):
        """ Send a LinkUp message when the connection is reconnected.

        :param connection: RabbitMQ's robust connection instance.
        """
        self.connection = connection
        self.link_down_reported = False

        # Notify the user of this class that the RabbitMQ communication is ready.
        await self.resp_queue.put({"msgType": "LinkUp", "host": self.host})

    # ---------------------------------------------------------
    #
    async def _initiate_communication(self):
        """ Establish communication with RabbitMQ (connection + channel).

        Send a LinkUp message when communication is established.
        """
        loop = asyncio.get_running_loop()

        # Create a RabbitMQ connection that automatically reconnects.
        self.connection: AbstractRobustConnection = await connect_robust(
            loop=loop, url=self.rabbit_url, virtualhost=f'/{config.env}')
        self.connection.reconnect_callbacks.add(self._on_connection_reconnected)
        self.connection.close_callbacks.add(self._on_connection_closed)

        self.link_down_reported = False
        self.host = self.connection.transport.connection.url.host

        # Create a publishing, or subscription channel.
        self.channel = await self.connection.channel()

        # To make sure the load is evenly distributed between the workers.
        await self.channel.set_qos(1)

        # Notify the user of this class that the RabbitMQ communication is ready.
        await self.resp_queue.put({"msgType": "LinkUp", "host": self.host})

    # ---------------------------------------------------------
    #
    async def _consume_incoming_message(self, message: AbstractIncomingMessage):
        """ Consume an incoming subscription message from RabbitMQ.

        :param message: A message that fits the subscription criteria.
        """

        if body := message.body:
            msg = json.loads(body)
            self.resp_queue.put_nowait(msg)
            await message.ack()

    # ---------------------------------------------------------
    #
    async def start_topic_subscription(self, routing_keys: list, permanent: bool = True):
        """ Start topic queue subscription using subscription_type context.

        When *permanent=True*, a **permanent** subscription is activated,
        otherwise a **temporary** subscription is activated.

        Send a LinkDown message when communication fails.

        :param routing_keys: Subscription topic(s).
        :param permanent: Indicate a wanted subscription type (default is True).
        :raise AssertError: When routing_keys is not a list.
        """
        assert isinstance(routing_keys, list), "routing_keys must be a list."

        try:
            subscription_type = ('permanent' if permanent else 'temporary')
            item = (subscription_type, config.env, routing_keys, self.host)
            logger.info('Starting {a} subscription for /{b}:{c} on {d}...',
                        **dict(list(zip('abcd', item))))
            await self.channel.declare_exchange(name=config.exchange,
                                                durable=True, type='topic')

            # Initiate a permanent subscription queue.
            if subscription_type == 'permanent':
                queue_name = f"{self.conf.program}@{self.conf.server}"
                queue = await self.channel.declare_queue(name=queue_name, durable=True)

            # Initiate a temporary subscription queue.
            else:
                queue_name = f"{self.conf.program}#temp#@{self.conf.server}"
                queue = await self.channel.declare_queue(name=queue_name, exclusive=True)

            # Add routing key(s) to subscription.
            for key in routing_keys:
                await queue.bind(exchange=config.exchange, routing_key=key)

            # Start consuming existing and future messages.
            state = subscription_type == 'temporary'
            await queue.consume(self._consume_incoming_message, exclusive=state, no_ack=False)

        except (exceptions.AMQPConnectionError, exceptions.ConnectionClosed,
                exceptions.AMQPChannelError, exceptions.ChannelClosed, exceptions.AMQPError) as why:
            msg = {"msgType": "LinkDown", "host": self.host, "why": f"{why.args[0]}"}
            logger.error(f'Topic subscription failed => {why.args[0]}')
            await self.resp_queue.put(msg)
            self.link_down_reported = True

    # ---------------------------------------------------------
    #
    async def start_permanent_work_subscription(self):
        """ Start a work queue subscription that survives when the subscription ends.

        Send a LinkDown message when communication fails.

        Work queue name is defined in self.conf.program during the class initialization.
        """
        try:
            queue = await self.channel.declare_queue(name=self.conf.program, durable=True)

            # Start consuming existing and future messages.
            await queue.consume(self._consume_incoming_message, no_ack=False)

        except (exceptions.AMQPConnectionError, exceptions.ConnectionClosed,
                exceptions.AMQPChannelError, exceptions.ChannelClosed, exceptions.AMQPError) as why:
            msg = {"msgType": "LinkDown", "host": self.host, "why": f"{why.args[0]}"}
            logger.error(f'Work subscription failed => {why.args[0]}')
            await self.resp_queue.put(msg)
            self.link_down_reported = True

    # ---------------------------------------------------------
    #
    async def publish_message(self, message: dict,
                              topic: Optional[str] = None,
                              work_queue_name: Optional[str] = None) -> bool:
        """ Publish a message on specified work, or topic queue.

        Send a LinkDown message when communication fails.

        :param message: Message to be published.
        :param topic: Used for topic queue messages.
        :param work_queue_name: Used for work queue messages.
        :return: Publisher delivery confirmation status.
        :raise AssertError: When a topic and work_queue_name are both empty.
        """
        assert (
                work_queue_name is not None or topic is not None
        ), "Both topic and work_queue_name values are EMPTY!"

        result = True
        confirmation: Union[Basic, DeliveredMessage] = None
        message_body = Message(
            content_type=CONTENT_TYPE,
            delivery_mode=DeliveryMode.PERSISTENT,
            body=json.dumps(message, ensure_ascii=False).encode())

        try:
            if topic:
                exchange = await self.channel.get_exchange(name=config.exchange)
                confirmation = await exchange.publish(
                    routing_key=topic, message=message_body)

            elif work_queue_name:
                confirmation = await self.channel.default_exchange.publish(
                    routing_key=work_queue_name, message=message_body)

            if not isinstance(confirmation, Basic.Ack):

                # This is OK, since we only need confirmation for
                # existing subscribers (they have defined routes).
                if confirmation.delivery.reply_text == 'NO_ROUTE':
                    result = True

                else:
                    result = False
                    logger.error('Message delivery was not '
                                 f'acknowledged by RabbitMQ => {confirmation!r}')

        except (exceptions.AMQPConnectionError, exceptions.ConnectionClosed,
                exceptions.AMQPChannelError, exceptions.ChannelClosed, exceptions.AMQPError) as why:
            msg = {"msgType": "LinkDown", "host": self.host, "why": f"{why.args[0]}"}
            logger.error(f'Delivery of {message["msgType"]} failed => {why.args[0]}')
            await self.resp_queue.put(msg)
            self.link_down_reported = True
            result = False

        return result

    # ----------------------------------------------------------
    #
    async def status_of(self):
        """ Send RabbitMQ connection status on the response queue. """
        status = self.connection and self.connection.connected.is_set()
        msg = {'msgType': 'StatusResponse',
               'resources': {'AsyncRabbitClient.connection': status}}
        self.resp_queue.put_nowait(msg)

    # ---------------------------------------------------------
    #
    async def start(self):
        """ Start the used resources in a controlled way. """

        await self._initiate_communication()

    # ---------------------------------------------------------
    #
    async def stop(self):
        """ Stop the used resources in a controlled way. """
        if self.connection:
            await self.connection.close()
