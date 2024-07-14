import os
import pika
import requests

from fastapi import FastAPI

rabbitmq_host = os.getenv('RABBITMQ_HOST')
rabbitmq_queue = os.getenv('RABBITMQ_QUEUE')
api_host = os.getenv('API_HOST')
api_port = os.getenv('API_HOST')

app = FastAPI()


@app.post("/{msg}")
def send_message(msg: str) -> dict:
    """
    Publish message in RabbitMQ

    Parameters
    -----------
    msg: A message to send to RabbitMQ broker

    Returns
    --------
    dict: A dictionary containing the sent message
    """

    # Connection to a broker
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(rabbitmq_host)
    )
    channel = connection.channel()

    # creating a que to which messages will be delivered
    # To ensure that the queue will survive a RabbitMQ node restart, specified 'durable'
    channel.queue_declare(queue=rabbitmq_queue, durable=True)

    # Publish the message onto the que
    channel.basic_publish(exchange='', routing_key=rabbitmq_queue, body=msg)

    # Close the connection to RabbitMQ
    connection.close()

    return {"Message": f"{msg}"}


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
def retrieve_message() -> dict:

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(rabbitmq_host)
    )

    channel = connection.channel()

    channel.queue_declare(queue=rabbitmq_queue)

    channel.basic_consume(
        queue=rabbitmq_queue,
        auto_ack=True,
        on_message_callback=on_message_received
    )

    print("started consuming")

    channel.start_consuming()

    return {"Message": "Success!"}
