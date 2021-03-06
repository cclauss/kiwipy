from __future__ import absolute_import
import logging
import uuid
from functools import partial
import sys
import traceback

from tornado import gen, concurrent
import topika

import kiwipy
from . import defaults
from . import messages
from . import utils

_LOGGER = logging.getLogger(__name__)

__all__ = ['RmqTaskSubscriber', 'RmqTaskPublisher']


class RmqTaskSubscriber(messages.BaseConnectionWithExchange):
    """
    Listens for tasks coming in on the RMQ task queue
    """

    def __init__(
            self,
            connection,
            task_queue_name=defaults.TASK_QUEUE,
            testing_mode=False,
            decoder=defaults.DECODER,
            encoder=defaults.ENCODER,
            exchange_name=defaults.MESSAGE_EXCHANGE,
            exchange_params=None,
            prefetch_size=defaults.TASK_PREFETCH_SIZE,
            prefetch_count=defaults.TASK_PREFETCH_COUNT,
    ):
        """
        :param connection: An RMQ connector
        :type connection: :class:`topika.Connection`
        :param task_queue_name: The name of the queue to use
        :param decoder: A message decoder
        :param encoder: A response encoder
        """
        super(RmqTaskSubscriber, self).__init__(
            connection, exchange_name=exchange_name, exchange_params=exchange_params)

        self._task_queue_name = task_queue_name
        self._testing_mode = testing_mode
        self._decode = decoder
        self._encode = encoder
        self._prefetch_size = prefetch_size
        self._prefetch_count = prefetch_count
        self._consumer_tag = None

        self._task_queue = None  # type: topika.Queue
        self._subscribers = []
        self._pending_tasks = []

    def add_task_subscriber(self, subscriber):
        self._subscribers.append(subscriber)

    def remove_task_subscriber(self, subscriber):
        self._subscribers.remove(subscriber)

    @gen.coroutine
    def connect(self):
        if self.channel():
            # Already connected
            return

        yield super(RmqTaskSubscriber, self).connect()
        yield self.channel().set_qos(prefetch_count=self._prefetch_count, prefetch_size=self._prefetch_size)

        # Set up task queue
        self._task_queue = yield self._channel.declare_queue(
            name=self._task_queue_name,
            durable=not self._testing_mode,
            auto_delete=self._testing_mode,
            arguments={"x-message-ttl": defaults.TASK_MESSAGE_TTL})
        # x-expires means how long does the queue stay alive after no clients
        # x-message-ttl means what is the default ttl for a message arriving in the queue
        yield self._task_queue.bind(self._exchange, routing_key=self._task_queue.name)

        self._consumer_tag = self._task_queue.consume(self._on_task)

    @gen.coroutine
    def _on_task(self, message):
        """
        :param message: The topika RMQ message
        :type message: :class:`topika.IncomingMessage`
        """
        with message.process(ignore_processed=True):
            handled = False
            task = self._decode(message.body)
            for subscriber in self._subscribers:
                try:
                    subscriber = utils.ensure_coroutine(subscriber)
                    result = yield subscriber(self, task)
                except kiwipy.TaskRejected:
                    continue
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    _LOGGER.debug('There was an exception in task %s:\n%s', exc, traceback.format_exc())
                    msg = self._build_response_message(utils.exception_response(sys.exc_info()[1:]), message)
                    handled = True  # Finished
                else:
                    # Create a reply message
                    msg = self._create_task_reply(message, result)
                    handled = True  # Finished

                if handled:
                    message.ack()
                    self._exchange.publish(msg, routing_key=message.reply_to)
                    break  # Done, do break the loop

            if not handled:
                # No one handled the task
                message.reject(requeue=True)

    def _create_task_reply(self, task_message, result):
        """
        Create the reply message based on the result of a task, if it's a future then
        a further message will be scheduled for when that future finishes (and so on
        if that future also resolves to a future)

        :param task_message: The original task message
        :param result: The result from the task
        :return: The reply message
        :rtype: :class:`topika.Message`
        """
        if isinstance(result, concurrent.Future):
            self._pending_tasks.append(result)

            def task_done(future):
                """
                Process this future being done
                :type future: :class:`tornado.concurrent.Future`
                """
                if future not in self._pending_tasks:
                    # Must have been 'cancelled'
                    return

                if future.cancelled():
                    reply_msg = self._build_response_message(utils.cancelled_response(), task_message)
                else:
                    try:
                        future_result = future.result()
                    except Exception as exception:  # pylint: disable=broad-except
                        reply_msg = self._build_response_message(utils.exception_response(exception), task_message)
                    else:
                        reply_msg = self._create_task_reply(task_message, future_result)

                # Clean up
                self._pending_tasks.remove(future)

                # Send the response to the sender
                self._loop.add_callback(partial(self._exchange.publish, reply_msg, routing_key=task_message.reply_to))

            result.add_done_callback(task_done)
            body = utils.pending_response()
        else:
            body = utils.result_response(result)

        return self._build_response_message(body, task_message)

    def _build_response_message(self, body, incoming_message):
        """
        Create a topika Message as a response to a task being deal with.

        :param body: The message body dictionary
        :type body: dict
        :param incoming_message: The original message we are responding to
        :type incoming_message: :class:`topika.IncomingMessage`
        :return: The response message
        :rtype: :class:`topika.Message`
        """
        # Add host info
        body[utils.HOST_KEY] = utils.get_host_info()
        message = topika.Message(body=self._encode(body), correlation_id=incoming_message.correlation_id)

        return message


class RmqTaskPublisher(messages.BasePublisherWithReplyQueue):
    """
    Publishes messages to the RMQ task queue and gets the response
    """

    def __init__(self,
                 connection,
                 task_queue_name=defaults.TASK_QUEUE,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 encoder=defaults.ENCODER,
                 decoder=defaults.DECODER,
                 confirm_deliveries=True,
                 testing_mode=False):
        super(RmqTaskPublisher, self).__init__(
            connection,
            exchange_name=exchange_name,
            exchange_params=exchange_params,
            encoder=encoder,
            decoder=decoder,
            confirm_deliveries=confirm_deliveries,
            testing_mode=testing_mode)
        self._task_queue_name = task_queue_name

    @gen.coroutine
    def task_send(self, msg):
        """
        Send a task for processing by a task subscriber
        :param msg: The task payload
        :return: A future representing the result of the task
        :rtype: :class:`tornado.concurrent.Future`
        """
        task_msg = topika.Message(
            body=self._encode(msg), correlation_id=str(uuid.uuid4()), reply_to=self._reply_queue.name)
        published, result_future = yield self.publish_expect_response(
            task_msg, routing_key=self._task_queue_name, mandatory=True)
        assert published, "The task was not published to the exchange"
        raise gen.Return(result_future)
