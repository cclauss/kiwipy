import abc
from future.utils import with_metaclass

__all__ = ['Communicator']


class Communicator(with_metaclass(abc.ABCMeta)):
    """
    The interface for a communicator used to both send and receive various
    types of message.
    """

    @abc.abstractmethod
    def add_rpc_subscriber(self, subscriber, identifier):
        pass

    @abc.abstractmethod
    def remove_rpc_subscriber(self, identifier):
        """
        Remove an RPC subscriber given the identifier.  Raises a `ValueError` if there
        is no such subscriber.

        :param identifier: The RPC subscriber identifier
        """
        pass

    @abc.abstractmethod
    def add_task_subscriber(self, subscriber):
        pass

    @abc.abstractmethod
    def remove_task_subscriber(self, subscriber):
        pass

    @abc.abstractmethod
    def add_broadcast_subscriber(self, subscriber):
        pass

    @abc.abstractmethod
    def remove_broadcast_subscriber(self, subscriber):
        pass

    @abc.abstractmethod
    def task_send(self, msg):
        """
        Send a task messages, this will be queued and picked up by a
        worker at some point in the future.  The method returns a future
        representing the outcome of the task.

        :param msg: The task message
        :return: A future corresponding to the outcome of the task
        :rtype: :class:`kiwi.Future`
        """

    def task_send_and_wait(self, msg):
        future = self.task_send(msg)
        self.await(future)
        return future.result()

    @abc.abstractmethod
    def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient.  This method
        returns a future representing the outcome of the call.

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        :rtype: :class:`kiwi.Future`
        """
        pass

    def rpc_send_and_wait(self, recipient_id, msg, timeout=None):
        future = self.rpc_send(recipient_id, msg)
        self.await(future, timeout=timeout)
        return future.result()

    @abc.abstractmethod
    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        pass

    @abc.abstractmethod
    def await(self, future=None, timeout=None):
        pass
