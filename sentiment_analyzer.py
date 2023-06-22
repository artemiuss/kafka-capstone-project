#!/usr/bin/env python3
import os, json, threading
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from kafka import KafkaConsumer
from kafka import KafkaProducer
from dotenv import load_dotenv

def process(kafka_host, kafka_port, kafka_topic):
    consumer = KafkaConsumer(
                                kafka_topic,
                                bootstrap_servers=[f"{kafka_host}:{kafka_port}"],
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=10000,
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                enable_auto_commit=False,
                                group_id='sentiment_analyzer'
                            )
    producer = KafkaProducer(bootstrap_servers=[f"{kafka_host}:{kafka_port}"])

    nltk.download('vader_lexicon')
    # Initialize the sentiment analyzer
    sid = SentimentIntensityAnalyzer()

    for message in consumer:
        #print (f"partition={message.partition}, offset={message.offset}, key={message.key}, timestamp={message.timestamp}")

        if message.value['body'] is None or len(message.value['body']) == 0:
            continue

        # Analyze sentiment
        sentiment_scores = sid.polarity_scores(message.value['body'])
        
        # Determine if the sentiment is positive or negative based on the compound score
        if sentiment_scores['compound'] >= 0.05:
            print("Positive sentiment" + "\n")
            message.value['sentiment'] = "1"
        elif sentiment_scores['compound'] <= -0.05:
            print("Negative sentiment" + "\n")
            message.value['sentiment'] = "-1"
        else:
            print("Neutral sentiment" + "\n")
            message.value['sentiment'] = "0"

        json_data = json.dumps(message.value).encode('utf-8')
        producer.send(topic="sentiments", value=json_data)
        producer.flush()
        consumer.commit()

    consumer.close()
    producer.close()

def main():
    load_dotenv()

    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    threads = []
    for i in range(0, 1):
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