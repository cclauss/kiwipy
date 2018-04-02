from contextlib import contextmanager
from enum import Enum
import logging
import pika
import pika.exceptions
import kiwipy
from tornado.gen import coroutine, Return

from . import loops
from . import tornado_connection

__all__ = ['RmqConnector', 'ConnectionListener']

LOGGER = logging.getLogger(__name__)


class ConnectionListener(object):
    def on_connection_opened(self, connector, connection):
        pass

    def on_connection_closed(self, connector, reconnecting):
        pass


class ConnectorState(Enum):
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2
    WAITING_TO_RECONNECT = 3


class RmqConnector(object):
    """
    A basic RMQ client that opens a connection and one channel.
    If an auto reconnect timeout is given it will try to keep the connection
    open by reopening if it is closed.
    """
    _connection = None

    def __init__(self,
                 amqp_url,
                 auto_reconnect_timeout=None,
                 loop=None):
        self._connection_params = pika.URLParameters(amqp_url)
        self._reconnect_timeout = auto_reconnect_timeout
        self._loop = loop if loop is not None else loops.new_event_loop()

        self._event_helper = kiwipy.EventHelper(ConnectionListener)
        self._running_future = None
        self._stopping = False

        self._disconnecting_future = None

        self._state = ConnectorState.DISCONNECTED

    @property
    def is_connected(self):
        return self._connection is not None and self._connection.is_open

    def loop(self):
        return self._loop

    def get_blocking_connection(self):
        return pika.BlockingConnection(self._connection_params)

    @contextmanager
    def blocking_channel(self, confirm_delivery=True):
        conn = self.get_blocking_connection()
        channel = conn.channel()
        if confirm_delivery:
            channel.confirm_delivery()
        yield channel
        channel.close()

    def get_connection_params(self):
        return self._connection_params

    @coroutine
    def get_connection(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        """
        LOGGER.info('Connecting to %s', self.get_connection_params())
        if self._state is ConnectorState.DISCONNECTED:
            self._state = ConnectorState.CONNECTING
            self._connection = tornado_connection.TornadoConnection(self._connection_params, self._loop)
            self._connection.add_on_open_callback(self._on_connection_open)
            self._connection.add_on_close_callback(self._on_connection_closed)

        yield self._connection.ensure_connected()
        raise Return(self._connection)

    @coroutine
    def disconnect(self):
        """This method closes the connection to RabbitMQ."""
        if self._state is not ConnectorState.DISCONNECTED:
            LOGGER.info('Closing connection')
            self._disconnecting_future = kiwipy.Future()
            yield self._connection.close()
            self._connection = None
            self._state = ConnectorState.DISCONNECTED

    def connection(self):
        return self._connection

    def add_connection_listener(self, listener):
        self._event_helper.add_listener(listener)

    def remove_connection_listener(self, listener):
        self._event_helper.remove_listener(listener)

    @coroutine
    def _on_connection_open(self, connection):
        """Called when the RMQ connection has been opened

        :type connection: pika.BaseConnection
        """
        LOGGER.info('Connection opened')
        self._connection = connection
        self._state = ConnectorState.CONNECTED

        self._event_helper.fire_event(ConnectionListener.on_connection_opened, self, connection)

    @coroutine
    def _on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._connection = None

        if self._state is not ConnectorState.DISCONNECTED and self._reconnect_timeout is not None:
            self._state = ConnectorState.WAITING_TO_RECONNECT
            LOGGER.warning(
                "Connection closed, reopening in {} seconds: ({}) {}".format(
                    self._reconnect_timeout, reply_code, reply_text
                ))
            self._loop.call_later(self._reconnect_timeout, self._reconnect)
        else:
            self._state = ConnectorState.DISCONNECTED
            if self._disconnecting_future:
                self._disconnecting_future.set_result(True)

        self._event_helper.fire_event(
            ConnectionListener.on_connection_closed, self,
            self._state == ConnectorState.WAITING_TO_RECONNECT)

    @coroutine
    def _reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        if not self._stopping:
            # Create a new connection
            yield self.get_connection()
