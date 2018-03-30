from tornado.gen import coroutine
import unittest
import uuid

try:
    import pika
    import pika.exceptions
    from kiwipy import rmq
except ImportError:
    pika = None

from . import utils


@unittest.skipIf(not pika, "Requires pika library and RabbitMQ")
class TestTornadoConnection(utils.TestCaseWithLoop):
    def setUp(self):
        super(TestTornadoConnection, self).setUp()
        self.connection = rmq.TornadoConnection(
            pika.URLParameters('amqp://localhost'), ioloop=self.loop)

    def tearDown(self):
        self.loop.run_sync(lambda: self.connection.close())
        super(TestTornadoConnection, self).tearDown()

    def test_simple_connect(self):
        """ Open a connection and a single channel"""

        @coroutine
        def run(connection):
            channel = yield connection.channel()
            self.assertTrue(channel.is_open)
            yield channel.close()
            self.assertTrue(channel.is_closed)

        self.loop.run_sync(lambda: run(self.connection))

    def test_open_existing_channel(self):
        """ Try opening the same channel number twice"""

        @coroutine
        def run(connection):
            yield connection.channel(1)
            with self.assertRaises((pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed)):
                # This should except and kill the connection
                yield connection.channel(1)
            self.assertTrue(self.connection.is_closed)

        self.loop.run_sync(lambda: run(self.connection))

    def test_publish_unknown_exchange(self):
        @coroutine
        def run(connection):
            channel = yield connection.channel()
            yield channel.confirm_delivery()
            with self.assertRaises(pika.exceptions.ChannelClosed):
                yield channel.publish(str(uuid.uuid4()), 'test', '')

        self.loop.run_sync(lambda: run(self.connection))
