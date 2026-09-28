import json
import os
import pika
rabbit_host = os.getenv("RABBITMQ_HOST")
exchange_name = os.getenv("RABBITMQ_EXCHANGE")
queue_name = os.getenv("RABBITMQ_QUEUE")
routing_key = os.getenv("RABBITMQ_ROUTING_KEY")
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host))
channel = connection.channel()
channel.exchange_declare(exchange=exchange_name,exchange_type="direct",durable=True)
channel.queue_declare(queue=queue_name,durable=True)
channel.queue_bind(exchange=exchange_name,queue=queue_name,routing_key=routing_key)
def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode("utf-8"))
        print()
        print("CRITICAL RECEIVED")
        print(message)
        print()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as error:
        print(f"Error: {error}")
        ch.basic_nack(delivery_tag=method.delivery_tag,requeue=False)
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name,on_message_callback=callback,auto_ack=False)
print("Waiting for critical messages...")
channel.start_consuming()

