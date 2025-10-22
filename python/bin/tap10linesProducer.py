# https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad
import os
from time import sleep
from json import dumps
from kafka import KafkaProducer

def on_success(record_metadata):
    print(f"Message sent successfully to {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")

def on_error(excp):
    print(f"Message send failed: {excp}")

topic = os.getenv("KAFKA_TOPIC", "tap")
pause = int(os.getenv("pause", 5))
producer = KafkaProducer(bootstrap_servers=['kafkaServer:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# Check if the producer is connected to any broker
if producer.bootstrap_connected():
    print("Connection to Kafka broker is active.")
else:
    print("Connection to Kafka broker failed.")

e=0
while(True):
    data = {'number' : e}
    print(f"Sending data: {data}")
    producer.send(topic, value=data).add_callback(on_success).add_errback(on_error)
    e+=1
    producer.flush()
    sleep(pause)
    

