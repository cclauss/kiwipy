# Defaults used for kiwipy exchanges, queues and routing keys.

TASK_EXCHANGE = 'kiwipy.tasks'
TASK_QUEUE = 'kiwipy.tasks'
MESSAGE_EXCHANGE = 'kiwipy.messages'
BROADCAST_TOPIC = '[broadcast]'
RPC_TOPIC = '[rpc]'
MESSAGE_TTL = 60000  # One minute
QUEUE_EXPIRES = 60000
REPLY_QUEUE_EXPIRES = 60000
TASK_MESSAGE_TTL = 60000 * 60 * 24 * 7  # One week
