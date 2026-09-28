import csv
import json
import os
import pika

rabbit_host = os.getenv("RABBITMQ_HOST")
exchange_name = os.getenv("RABBITMQ_EXCHANGE")
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host))
channel = connection.channel()

channel.exchange_declare(exchange=exchange_name,exchange_type="direct",durable=True)

# -------------------------
# Queues
# -------------------------
channel.queue_declare(queue="warning-queue",durable=True)
channel.queue_declare(queue="critical-queue",durable=True)
# -------------------------
# Bindings
# -------------------------
channel.queue_bind(exchange=exchange_name,queue="warning-queue",routing_key="alert.warning")
channel.queue_bind(exchange=exchange_name,queue="critical-queue",routing_key="alert.critical")

# -------------------------
# פונקציה לשליחת הודעה
# -------------------------
def send_message(message):
    value = int(message["value"])
    if value >= 90:
        routing_key = "alert.critical"
        message["status"] = "Critical"
    else:
        routing_key = "alert.warning"
        message["status"] = "Warning"
    channel.basic_publish(exchange=exchange_name,routing_key=routing_key,body=json.dumps(message),properties=pika.BasicProperties(delivery_mode=2))
    print(f"Sent {message['status']}: {message['id']} - {value}")
# -------------------------
# JSON
# -------------------------
with open("events.json","r",encoding="utf-8") as file:
    json_messages = json.load(file)
for message in json_messages:
    send_message(message)
print(f"Finished JSON: {len(json_messages)} messages")
# -------------------------
# CSV
# -------------------------
with open("events.csv","r",encoding="utf-8") as file:
    reader = csv.DictReader(file)
    csv_messages = list(reader)
for message in csv_messages:
    # CSV מחזיר הכל בתור string
    message["value"] = int(message["value"])
    send_message(message)
print(f"Finished CSV: {len(csv_messages)} messages")
connection.close()
print("Producer finished")