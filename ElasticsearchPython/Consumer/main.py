import json
import os

import pika
from elasticsearch import Elasticsearch


# --------------------------------------------------
# Elasticsearch settings
# --------------------------------------------------

elastic_url = os.getenv("ELASTICSEARCH_URL")
index_name = os.getenv("ELASTICSEARCH_INDEX")

elastic = Elasticsearch(elastic_url)


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

    rabbit_host = os.getenv("RABBITMQ_HOST")
    rabbit_exchange = os.getenv("RABBITMQ_EXCHANGE")
    rabbit_queue = os.getenv("RABBITMQ_QUEUE")
    rabbit_routing_key = os.getenv("RABBITMQ_ROUTING_KEY")


    # מתחבר ל-RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbit_host
        )
    )

    channel = connection.channel()


    # יוצר Exchange
    channel.exchange_declare(
        exchange=rabbit_exchange,
        exchange_type="direct",
        durable=True
    )


    # יוצר Queue
    channel.queue_declare(
        queue=rabbit_queue,
        durable=True
    )


    # מחבר את ה-Queue ל-Exchange
    channel.queue_bind(
        exchange=rabbit_exchange,
        queue=rabbit_queue,
        routing_key=rabbit_routing_key
    )


    # מקבל הודעה אחת בכל פעם
    channel.basic_qos(
        prefetch_count=1
    )


    # רץ בכל פעם שמגיעה הודעה
    def callback(ch, method, properties, body):

        try:

            # ממיר את ההודעה ל-dictionary
            message = json.loads(
                body.decode("utf-8")
            )


            print(
                f"RabbitMQ received: {message['id']}"
            )


            # שומר ל-Elasticsearch
            save_to_elasticsearch(message)


            # ACK אחרי שהשמירה הצליחה
            ch.basic_ack(
                delivery_tag=method.delivery_tag
            )


        except Exception as error:

            print(
                f"RabbitMQ Error: {error}"
            )


            # אם הייתה שגיאה
            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False
            )


    # מתחיל לצרוך מה-Queue
    channel.basic_consume(
        queue=rabbit_queue,
        on_message_callback=callback,
        auto_ack=False
    )


    print("Waiting for RabbitMQ messages...")


    # משאיר את ה-Consumer פעיל
    channel.start_consuming()


# --------------------------------------------------
# Kafka Consumer
# כרגע בהערה עד שיהיה Producer ששולח ל-Kafka
# --------------------------------------------------

# from confluent_kafka import Consumer


# def start_kafka_consumer():

#     kafka_bootstrap = os.getenv(
#         "KAFKA_BOOTSTRAP_SERVERS"
#     )

#     kafka_topic = os.getenv(
#         "KAFKA_TOPIC"
#     )

#     kafka_group = os.getenv(
#         "KAFKA_GROUP_ID"
#     )


#     consumer = Consumer({
#         "bootstrap.servers": kafka_bootstrap,
#         "group.id": kafka_group,
#         "auto.offset.reset": "earliest",
#         "enable.auto.commit": False
#     })


#     consumer.subscribe(
#         [kafka_topic]
#     )


#     print("Waiting for Kafka messages...")


#     while True:

#         kafka_message = consumer.poll(1.0)


#         if kafka_message is None:
#             continue


#         if kafka_message.error():

#             print(
#                 kafka_message.error()
#             )

#             continue


#         try:

#             message = json.loads(
#                 kafka_message.value().decode(
#                     "utf-8"
#                 )
#             )


#             print(
#                 f"Kafka received: {message['id']}"
#             )


#             save_to_elasticsearch(message)


#             consumer.commit(
#                 message=kafka_message,
#                 asynchronous=False
#             )


#         except Exception as error:

#             print(
#                 f"Kafka Error: {error}"
#             )


# --------------------------------------------------
# Main
# --------------------------------------------------

# כרגע מפעילים רק RabbitMQ
start_rabbit_consumer()


# כשנרצה להחזיר את Kafka,
# נשתמש שוב ב-thread עבור RabbitMQ
# ונפעיל את Kafka בתהליך הראשי