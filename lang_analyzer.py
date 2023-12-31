#!/usr/bin/env python3
import os, json, threading
from kafka import KafkaConsumer
from kafka import KafkaProducer
from dotenv import load_dotenv
from langdetect import detect
from iso639 import languages

def process(kafka_host, kafka_port, kafka_topic):
    consumer = KafkaConsumer(
                                kafka_topic,
                                bootstrap_servers=[f"{kafka_host}:{kafka_port}"],
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=10000,
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                enable_auto_commit=False,
                                group_id='lang_analyzer'
                            )
    producer = KafkaProducer(bootstrap_servers=[f"{kafka_host}:{kafka_port}"])

    for message in consumer:
        #print (f"partition={message.partition}, offset={message.offset}, key={message.key}, timestamp={message.timestamp}")
        
        if message.value['body'] is None or len(message.value['body']) == 0:
            continue

        # Detect language
        try:
            detect_result = detect(message.value['body'])
            message.value['lang'] = languages.get(part1=detect_result).name
            print(message.value['lang'] + "\n")
            json_data = json.dumps(message.value).encode('utf-8')
            producer.send(topic="langs", value=json_data)
            producer.flush()
        except:
            continue
        consumer.commit()

    consumer.close()
    producer.close()

def main():
    load_dotenv()

    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    threads = []
    for i in range(0, 8):
        t = threading.Thread(
                                target=process,
                                args=(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC,)
                            )
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

if __name__ == '__main__':
    main()