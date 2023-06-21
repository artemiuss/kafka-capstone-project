#!/usr/bin/env python3
import os, json
from kafka import KafkaConsumer
from dotenv import load_dotenv
from collections import Counter

def main():
    load_dotenv()

    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")

    topics = ["langs", "keywords", "sentiments"]

    consumer = KafkaConsumer(
                                bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=10000,
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                enable_auto_commit=False
                            )
    consumer.subscribe(topics=topics)

    lang_cnt = Counter()
    keyword_cnt = Counter()
    sentiment_cnt = Counter()

    for message in consumer:
        print (f"topic={message.topic}, partition={message.partition}, offset={message.offset}, key={message.key}, timestamp={message.timestamp}")
        if message.topic == "langs":
            lang_cnt.update({message.value['lang']: 1})
            print(lang_cnt)
        elif message.topic == "keywords":
            for keyword in message.value['keywords']:
                keyword_cnt.update({keyword[0]: 1})
            print(keyword_cnt)
        elif message.topic == "sentiments":
            if message.value['sentiment'] == '1':
                sentiment = "positive"
            elif message.value['sentiment'] == '0':
                sentiment = "neutral"
            elif message.value['sentiment'] == '-1':
                sentiment = "negative"
            
            sentiment_cnt.update({sentiment: 1})
            print(sentiment_cnt)
        
    consumer.close()

if __name__ == '__main__':
    main()