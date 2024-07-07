import pika 
import requests


class RabbitMQBroker:
    """
    RabbitMQ message broker

    Parameters
    -----------
    hostname: Host connection to RabbitMQ
    routing_key: The queue the message should go to
    exchange: A binding to the message queue. i.e. messages
        are sent through to an exchange
    """

    def __init__(self, hostname: str, routing_key: str, exchange: str='') -> None:

        self.routing_key = routing_key
        self.exchange = exchange

        # Connecting to a broker on our local machine, hence 'localhost'
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(hostname)
        )
        self.channel = self.connection.channel()
        # creating a que to which messages will be delivered
        # To ensure that the queue will survive a RabbitMQ node restart, specified 'durable'
        self.channel.queue_declare(queue=routing_key, durable=True)
    
    
    def publish_message(self, msg: str) -> None:
        """
        Publish message in RabbitMQ

        Parameters
        -----------
        msg: message to be submitted in a que
        """

        self.channel.basic_publish(
            exchange='',
            routing_key=self.routing_key,
            body=msg,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            )
        )

        self.connection.close()

    
    def consume_message(self, callback) -> None:
        """
        Consume the published message
        """

        # Specify that the particular callback function 
        # should receive messages from the queue
        self.channel.basic_consume(
            queue=self.routing_key, 
            on_message_callback=callback
        )

        self.channel.start_consuming()