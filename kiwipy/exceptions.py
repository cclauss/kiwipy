__all__ = ['RemoteException', 'DeliveryFailed', 'UnroutableError', 'TaskRejected',
           'TimeoutError']


class RemoteException(Exception):
    """ An exception occurred at the remote end of the call """
    pass


class DeliveryFailed(Exception):
    """ Failed to deliver a message """
    pass


class UnroutableError(DeliveryFailed):
    """ The messages was unroutable """
    pass


class TaskRejected(Exception):
    """ A task was rejected at the remote end """
    pass


class TimeoutError(Exception):
    """ Waiting for a future timed out """
    pass
