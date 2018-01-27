from kiwipy.rmq import *

communicator = RmqCommunicator(RmqConnector('amqp://localhost'))

# Send an RPC message
for _ in range(10000):
    print(" [x] Requesting fib(30)")
    response = communicator.rpc_send_and_wait('fib', 30)
    print(" [.] Got %r" % response)
