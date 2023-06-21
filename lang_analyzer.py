#!/usr/bin/env python3
import os, json
from kafka import KafkaConsumer
from kafka import KafkaProducer
from dotenv import load_dotenv
from langdetect import detect
from iso639 import languages

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
        #print (f"partition={message.partition}, offset={message.offset}, key={message.key}, timestamp={message.timestamp}")
        
        if message.value['body'] is None:
            continue

        # Detect language
        detect_result = detect(message.value['body'])
        message.value['lang'] = languages.get(part1=detect_result).name
        print(message.value['lang'])
        json_data = json.dumps(message.value).encode('utf-8')
        producer.send(topic="langs", value=json_data)
        producer.flush()

    consumer.close()
    producer.close()

if __name__ == '__main__':
    main()