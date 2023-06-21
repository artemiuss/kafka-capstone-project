#!/usr/bin/env python3
import os, json
from kafka import KafkaConsumer
from dotenv import load_dotenv

def main():
    load_dotenv()

    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")

    topics = [ "langs", "keywords", "sentiments"]
    consumer = KafkaConsumer(
                                topics,
                                bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=10000,
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                enable_auto_commit=False
                            )
    for message in consumer:
        if message.topic == "langs":
            pass
        elif message.topic == "keywords":
            pass
        elif message.topic == "sentiments":
            pass

        print (f"partition={message.partition}, offset={message.offset}, key={message.key}, timestamp={message.timestamp}")
    consumer.close()

if __name__ == '__main__':
    main()