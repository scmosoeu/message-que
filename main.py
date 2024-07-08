import os
import requests

from fastapi import FastAPI

from message_broker.broker import RabbitMQBroker

rabbitmq_host = os.getenv('RABBITMQ_HOST')
rabbitmq_queue = os.getenv('RABBITMQ_QUEUE')
api_host = os.getenv('API_HOST')
api_port = os.getenv('API_HOST')


app = FastAPI()

rabbitmq_host = os.getenv('RABBITMQ_HOST')
rabbitmq_queue = os.getenv('RABBITMQ_QUEUE')

@app.post("/")
def send_message(msg: str) -> None:
    """
    Publish message in RabbitMQ

    Parameters
    -----------
    msg: A message to send to RabbitMQ broker
    """

    rmq = RabbitMQBroker(
        hostname=rabbitmq_host,
        routing_key=rabbitmq_queue
    )

    rmq.publish_message(msg)


def on_message_received(ch, method, properties, body):
    """
    Subscribe a callback function to a queue
    """
    commodity = body.decode('utf-8')

    url = f"http://{api_host}:{api_port}/{commodity}"

    commodity_response = requests.get(url)

    res = commodity_response.json()

    print(res)

@app.get("/")
def retrieve_message():

    rmq = RabbitMQBroker(
        hostname=rabbitmq_host,
        routing_key=rabbitmq_queue
    )

    rmq.consume_message(callback=on_message_received)
