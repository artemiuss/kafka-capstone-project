#!/usr/bin/env python3
import os, json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
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

    nltk.download('vader_lexicon')
    # Initialize the sentiment analyzer
    sid = SentimentIntensityAnalyzer()

    for message in consumer:
        #print (f"partition={message.partition}, offset={message.offset}, key={message.key}, timestamp={message.timestamp}")

        if message.value['body'] is None or message.value['body'].length == 0:
            continue

        # Analyze sentiment
        sentiment_scores = sid.polarity_scores(message.value['body'])
        
        # Determine if the sentiment is positive or negative based on the compound score
        if sentiment_scores['compound'] >= 0.05:
            print("Positive sentiment")
            message.value['sentiment'] = "1"
        elif sentiment_scores['compound'] <= -0.05:
            print("Negative sentiment")
            message.value['sentiment'] = "-1"
        else:
            print("Neutral sentiment")
            message.value['sentiment'] = "0"

        json_data = json.dumps(message.value).encode('utf-8')
        producer.send(topic="sentiments", value=json_data)
        producer.flush()

    consumer.close()
    producer.close()

if __name__ == '__main__':
    main()