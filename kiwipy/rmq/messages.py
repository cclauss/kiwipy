import abc
from collections import deque, namedtuple
from future.utils import with_metaclass
import kiwipy
from past.builtins import basestring
import pika
import uuid
import yaml

from . import pubsub
from . import defaults
from . import utils


class Publisher(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def publish_msg(self, body, routing_key, correlation_id=None, ttl=None):
        """
        Publish a message with a routing key. A correlation id and time-to-live
        can optionally be supplied.

        :param body: The body of the message
        :param routing_key: The routing key
        :type routing_key: str
        :param correlation_id: The correlation id
        :type correlation_id: str
        :param ttl: A time-to-live for the message in seconds
        :type ttl: str or float
        """
        pass

    @abc.abstractmethod
    def await_response(self, correlation_id, callback):
        """
        Away a response for a message with a given correlation id and call the
        callback with the response message

        :param correlation_id: The message correlation id
        :param callback: The callback function that will be given the message
            body as the sole parameter
        """
        pass


class Message(with_metaclass(abc.ABCMeta)):
    @abc.abstractproperty
    def future(self):
        pass

    @abc.abstractmethod
    def send(self, publisher):
        """
        Send the message using the supplied publisher
        :return:
        """
        pass


class RpcMessage(Message):
    """
    A Remote Procedure Call message that waits for a response from the recipient.
    """

    def __init__(self, recipient_id, body):
        self.recipient_id = recipient_id
        self.body = body
        self.correlation_id = str(uuid.uuid4())
        self._future = kiwipy.Future()
        self._publisher = None

    @property
    def future(self):
        return self._future

    def send(self, publisher):
        self._publisher = publisher
        routing_key = "{}.{}".format(defaults.RPC_TOPIC, self.recipient_id)
        publisher.publish_msg(
            self.body,
            routing_key,
            self.correlation_id,
            mandatory=True)
        self._publisher.await_response(self.correlation_id, self.on_response)
        return self.future

    def on_response(self, done_future):
        kiwipy.copy_future(done_future, self.future)


class BroadcastMessage(Message):
    BODY = 'body'
    SENDER = 'sender'
    SUBJECT = 'subject'
    CORRELATION_ID = 'correlation_id'

    def __init__(self, body, sender=None, subject=None, correlation_id=None):
        self.message = {
            BroadcastMessage.BODY: body,
            BroadcastMessage.SENDER: sender,
            BroadcastMessage.SUBJECT: subject,
            BroadcastMessage.CORRELATION_ID: correlation_id,
        }
        self._future = kiwipy.Future()

    @property
    def future(self):
        return self._future

    def send(self, publisher):
        publisher.publish_msg(self.message, defaults.BROADCAST_TOPIC)


class BaseConnectionWithExchange(utils.InitialisationMixin, pubsub.ConnectionListener):
    """
    An RMQ connection with an exchange
    """
    DEFAULT_EXCHANGE_PARAMS = {
        'exchange_type': 'topic',
        'auto_delete': True
    }

    def __init__(self, connector,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None):
        super(BaseConnectionWithExchange, self).__init__()

        if exchange_params is None:
            exchange_params = self.DEFAULT_EXCHANGE_PARAMS

        self._connector = connector
        self._exchange_name = exchange_name
        self._exchange_params = exchange_params

        self._reset_channel()
        connector.add_connection_listener(self)
        if connector.is_connected:
            connector.open_channel(self.on_channel_open)

    def on_connection_opened(self, connector, connection):
        connector.open_channel(self.on_channel_open)

    def get_exchange_name(self):
        return self._exchange_name

    def get_channel(self):
        return self._channel

    def close(self):
        self._connector.remove_connection_listener(self)
        if self._channel is not None:
            self._connector.close_channel(self.get_channel())
        self._connector = None
        self._channel = None

    # region RMQ communications
    def _reset_channel(self):
        """ Reset all channel specific members """
        self._channel = None
        self.reinitialising()

    @utils.initialiser()
    def on_channel_open(self, channel):
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_close)
        channel.exchange_declare(
            self.on_exchange_declareok, exchange=self.get_exchange_name(),
            **self._exchange_params)

    def _on_channel_close(self, channel, reply_code, reply_text):
        self._reset_channel()

    @utils.initialiser()
    def on_exchange_declareok(self, unused_frame):
        pass


DeliveryInfo = namedtuple('DeliveryInfo', ['tag', 'correlation_id', 'future'])


class BasePublisherWithReplyQueue(
    utils.InitialisationMixin, pubsub.ConnectionListener, Publisher):
    """

    """
    DEFAULT_EXCHANGE_PARAMS = {
        'exchange_type': 'topic',
        'auto_delete': True
    }

    def __init__(self, connector,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 encoder=yaml.dump,
                 decoder=yaml.load,
                 confirm_deliveries=True,
                 publish_connection=None):
        """

        :param connector:
        :param exchange_name:
        :param exchange_params:
        :param encoder:
        :param decoder:
        :param confirm_deliveries:
        :param publish_connection: A blocking connection used for publishing
            messages to the exchange
        :type publish_connection: :class:`pika.BlockingConnection`
        """
        super(BasePublisherWithReplyQueue, self).__init__()

        if exchange_params is None:
            exchange_params = self.DEFAULT_EXCHANGE_PARAMS

        self._exchange_name = exchange_name
        self._exchange_params = exchange_params
        self._encode = encoder
        self._response_decode = decoder
        self._confirm_deliveries = confirm_deliveries

        self._queued_messages = []
        self._awaiting_response = {}
        self._returned_messages = set()

        self._reply_queue_name = "{}-{}".format(self._exchange_name, str(uuid.uuid4()))

        if publish_connection is None:
            publish_connection = pika.BlockingConnection(connector.get_connection_params())
        self._publish_channel = self.create_publish_channel(publish_connection)

        self._reset_channel()
        self._connector = connector
        connector.add_connection_listener(self)
        if connector.is_connected:
            connector.open_channel(self.on_channel_open)

    def create_publish_channel(self, connection):
        channel = connection.channel()
        channel.confirm_delivery()
        channel.exchange_declare(exchange=self.get_exchange_name(), **self._exchange_params)

        return channel

    def action_message(self, message):
        """
        Execute a message that involves communication.  This could mean that the
        message gets queued first and then sent as soon as the connection is open.
        In any case the method returns a future for the message.

        :param message: The message to execute
        :return: A future corresponding to action
        :rtype: :class:`kiwi.Future`
        """
        self._connector.connect()
        if self.initialised_future().done():
            message.send(self)
        else:
            self._queued_messages.append(message)
        return message.future

    def await_response(self, correlation_id, callback):
        self._awaiting_response[correlation_id] = callback

    def publish_msg(self, msg, routing_key, correlation_id=None, mandatory=False, ttl=None):
        # pika (and AMQP) expects the ttl to be a string
        if ttl is not None and not isinstance(ttl, basestring):
            ttl = str(ttl)

        return self.do_publish(
            correlation_id,
            exchange=self._exchange_name,
            routing_key=routing_key,
            properties=pika.BasicProperties(
                reply_to=self._reply_queue_name,
                correlation_id=correlation_id,
                delivery_mode=1,
                content_type='text/json',
                expiration=ttl,
            ),
            body=self._encode(msg),
            mandatory=mandatory
        )

    def do_publish(self, correlation_id, *args, **kwargs):
        if self._confirm_deliveries and correlation_id is None:
            # Give a temporary ID to be able to keep track of returned messages
            correlation_id = str(uuid.uuid4())

        try:
            properties = kwargs['properties']
        except KeyError:
            properties = pika.BasicProperties()

        properties.correlation_id = correlation_id

        # self._channel.basic_publish(*args, **kwargs)
        self._publish_channel.basic_publish(*args, **kwargs)
        delivery_future = None

        if self._confirm_deliveries:
            delivery_future = kiwipy.Future()
            self._num_published += 1
            self._delivery_info.append(DeliveryInfo(self._num_published, correlation_id, delivery_future))

        return delivery_future

    def on_connection_opened(self, connector, connection):
        connector.open_channel(self.on_channel_open)

    def close(self):
        self._connector.remove_connection_listener(self)
        if self.get_channel() is not None:
            self._connector.close_channel(self.get_channel())
        self._channel = None
        self._connector = None

    def get_reply_queue_name(self):
        return self._reply_queue_name

    def get_exchange_name(self):
        return self._exchange_name

    def get_channel(self):
        return self._channel

    def _on_response(self, ch, method, props, body):
        """ Called when we get a message on our response queue """
        correlation_id = props.correlation_id
        try:
            callback = self._awaiting_response[correlation_id]
        except KeyError:
            # TODO: Log
            pass
        else:
            response = self._response_decode(body)
            response_future = kiwipy.Future()
            utils.response_to_future(response, response_future)
            if response_future.done():
                self._awaiting_response.pop(correlation_id)
                callback(response_future)
            else:
                pass  # Keep waiting

    def _send_queued_messages(self):
        for message in self._queued_messages:
            message.send(self)
        self._queued_messages = []

    # region RMQ communications
    def _reset_channel(self):
        """ Reset all channel specific members """
        self._channel = None
        if self._confirm_deliveries:
            self._num_published = 0
            self._delivery_info = deque()

        self.reinitialising()

        # Send messages when ready
        self.initialised_future().add_done_callback(lambda x: self._send_queued_messages())

    @utils.initialiser()
    def on_channel_open(self, channel):
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_close)
        channel.exchange_declare(
            self.on_exchange_declareok, exchange=self.get_exchange_name(),
            **self._exchange_params)

        # Declare the reply queue
        channel.queue_declare(
            self._on_reply_queue_declareok,
            queue=self._reply_queue_name,
            exclusive=True,
            auto_delete=True)

    def _on_channel_close(self, channel, reply_code, reply_text):
        self._reset_channel()

    @utils.initialiser()
    def on_exchange_declareok(self, frame):
        pass

    @utils.initialiser()
    def _on_reply_queue_declareok(self, frame):
        self._reply_queue_name = frame.method.queue
        self._channel.basic_consume(
            self._on_response, no_ack=True, queue=self._reply_queue_name)


        # endregion
