import os

import redis
import requests

from message_broker.broker import RabbitMQBroker

rabbitmq_host = os.getenv('RABBITMQ_HOST')
rabbitmq_queue = os.getenv('RABBITMQ_QUEUE')
api_host = os.getenv('API_HOST')
api_port = os.getenv('API_HOST')
redis_host = os.getenv('REDIS_HOST')
redis_port = os.getenv('REDIS_PORT')


def on_message_received(ch, method, properties, body):
    """
    Subscribe a callback function to a queue
    """
    commodity = body.decode('utf-8')

    url = f"http://{api_host}:{api_port}/{commodity}"
    
    commodity_response = requests.get(url)

    res = commodity_response.json()

    r = redis.Redis(
        host=redis_host,
        port=redis_port,
        decode_responses=True
    )

    r.set('information_date', res['information_date'])
    r.hset(res['daily_prices']['commodity'], mapping=res['daily_prices'])

rmq = RabbitMQBroker(
    hostname=rabbitmq_host,
    routing_key=rabbitmq_queue
) 

rmq.consume_message(callback=on_message_received)