import os 

from message_broker.broker import RabbitMQBroker

rabbitmq_host = os.getenv('RABBITMQ_HOST')
rabbitmq_queue = os.getenv('RABBITMQ_QUEUE')

rmq = RabbitMQBroker(
    hostname=rabbitmq_host,
    routing_key=rabbitmq_queue
)

rmq.publish_message('apples')
