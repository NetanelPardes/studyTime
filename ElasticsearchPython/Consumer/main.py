import json
import os
import time

import pika
from elasticsearch import Elasticsearch


# --------------------------------------------------
# Elasticsearch settings
# --------------------------------------------------

elastic_url = os.getenv("ELASTICSEARCH_URL")
index_name = os.getenv("ELASTICSEARCH_INDEX")

if not elastic_url:
    raise Exception("ELASTICSEARCH_URL missing")

if not index_name:
    raise Exception("ELASTICSEARCH_INDEX missing")


# יוצר חיבור ל-Elasticsearch
elastic = Elasticsearch(
    elastic_url
)


# --------------------------------------------------
# פונקציה ששומרת ל-Elasticsearch
# --------------------------------------------------

def save_to_elasticsearch(message):

    elastic.index(
        index=index_name,
        id=message["id"],
        document=message
    )

    print(
        f"Saved to Elasticsearch: {message['id']}"
    )


# --------------------------------------------------
# RabbitMQ Consumer
# --------------------------------------------------

def start_rabbit_consumer():

    # כתובת RabbitMQ
    rabbit_host = os.getenv("RABBITMQ_HOST")

    # הפורט של RabbitMQ
    rabbit_port = int(
        os.getenv("RABBITMQ_PORT", "5672")
    )

    # שם ה-Exchange
    rabbit_exchange = os.getenv(
        "RABBITMQ_EXCHANGE"
    )

    # שם ה-Queue
    rabbit_queue = os.getenv(
        "RABBITMQ_QUEUE"
    )

    # Routing Key
    rabbit_routing_key = os.getenv(
        "RABBITMQ_ROUTING_KEY"
    )


    if not rabbit_host:
        raise Exception("RABBITMQ_HOST missing")

    if not rabbit_exchange:
        raise Exception("RABBITMQ_EXCHANGE missing")

    if not rabbit_queue:
        raise Exception("RABBITMQ_QUEUE missing")

    if not rabbit_routing_key:
        raise Exception("RABBITMQ_ROUTING_KEY missing")


    # --------------------------------------------------
    # Connection
    # --------------------------------------------------

    # מנסה להתחבר שוב ושוב ל-RabbitMQ
    # עד שהחיבור מצליח
    while True:

        try:

            print(
                f"Connecting to RabbitMQ: "
                f"{rabbit_host}:{rabbit_port}"
            )

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbit_host,
                    port=rabbit_port
                )
            )

            print(
                "Connected to RabbitMQ"
            )

            break


        except pika.exceptions.AMQPConnectionError:

            print(
                "RabbitMQ is not ready. "
                "Retrying in 5 seconds..."
            )

            time.sleep(5)


    # יוצר Channel
    channel = connection.channel()


    # --------------------------------------------------
    # Exchange
    # --------------------------------------------------

    # יוצר או מוודא שה-Exchange קיים
    channel.exchange_declare(
        exchange=rabbit_exchange,
        exchange_type="direct",
        durable=True
    )


    # --------------------------------------------------
    # Queue
    # --------------------------------------------------

    # יוצר Queue נפרדת עבור Elasticsearch
    channel.queue_declare(
        queue=rabbit_queue,
        durable=True
    )


    # --------------------------------------------------
    # Binding
    # --------------------------------------------------

    # מחבר את ה-Queue ל-Exchange לפי Routing Key
    channel.queue_bind(
        exchange=rabbit_exchange,
        queue=rabbit_queue,
        routing_key=rabbit_routing_key
    )


    # --------------------------------------------------
    # QoS
    # --------------------------------------------------

    # מקבל הודעה אחת בכל פעם
    channel.basic_qos(
        prefetch_count=1
    )


    # --------------------------------------------------
    # Callback
    # --------------------------------------------------

    def callback(
        ch,
        method,
        properties,
        body
    ):

        try:

            # הופך את ההודעה מ-bytes ל-dictionary
            message = json.loads(
                body.decode("utf-8")
            )


            print(
                f"RabbitMQ received: {message['id']}"
            )


            # שומר את ההודעה ב-Elasticsearch
            save_to_elasticsearch(
                message
            )


            # שולח ACK רק אחרי שהשמירה הצליחה
            ch.basic_ack(
                delivery_tag=method.delivery_tag
            )


        except Exception as error:

            print(
                f"RabbitMQ Error: {error}"
            )


            # שולח NACK אם הייתה שגיאה
            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False
            )


    # --------------------------------------------------
    # Start Consumer
    # --------------------------------------------------

    # מתחיל לצרוך הודעות מה-Queue
    channel.basic_consume(
        queue=rabbit_queue,
        on_message_callback=callback,
        auto_ack=False
    )


    print(
        "Waiting for RabbitMQ messages..."
    )


    # משאיר את ה-Consumer פעיל
    channel.start_consuming()


# --------------------------------------------------
# Main
# --------------------------------------------------

start_rabbit_consumer()