#!/usr/bin/env python3
import sys, os, time, json, datetime
from kafka import KafkaConsumer
from kafka import KafkaProducer
from dotenv import load_dotenv

def main():
    load_dotenv()

    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    consumer = KafkaConsumer(
                                KAFKA_TOPIC,
                                bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=10000,
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                enable_auto_commit=False
                            )
    producer = KafkaProducer(bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"])
   
    for message in consumer:
        print (f"partition={message.partition}, offset={message.offset}, key={message.key}, timestamp={message.timestamp}")
        #print (f"value={message.value}")
        json_data
        row['lang'] = ...
        producer.send(topic=KAFKA_TOPIC, value=json_data)
        producer.flush()

    consumer.close()
    producer.close()

if __name__ == '__main__':
    main()