#!/usr/bin/env python3
import os, json, threading
from kafka import KafkaConsumer
from dotenv import load_dotenv
from collections import Counter

lang_cnt = Counter()
keyword_cnt = Counter()
sentiment_cnt = Counter()

lang_i = 0
keyword_i = 0
sentiment_i = 0

# Lock for synchronization
lock = threading.Lock()

def consume(kafka_host, kafka_port, kafka_topic):
    global lang_i
    global keyword_i
    global sentiment_i

    consumer = KafkaConsumer(
                                kafka_topic,
                                bootstrap_servers=[f"{kafka_host}:{kafka_port}"],
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=10000,
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                enable_auto_commit=False,
                                group_id='my-group'
                            )

    for message in consumer:
        #print (f"topic={message.topic}, partition={message.partition}, offset={message.offset}, key={message.key}, timestamp={message.timestamp}")
        if message.topic == "langs":
            with lock:
                lang_cnt.update({message.value['lang']: 1})
                
            print("[LANGUAGES] "+ ", ".join(f"{key}: {value}" for key, value in lang_cnt.items()))
            
            with lock:
                lang_i += 1
        elif message.topic == "keywords":
            for keyword in message.value['keywords']:
                with lock:
                    keyword_cnt.update({keyword[0]: 1})

            keyword_cnt_top = sorted(keyword_cnt.items(), key=lambda x: x[1], reverse=True)[:10]
            print("[KEYWORDS] "+ ", ".join(str(f"{x[0]}: {x[1]}") for x in keyword_cnt_top))
            
            with lock:
                keyword_i += 1
        elif message.topic == "sentiments":
            if message.value['sentiment'] == '1':
                sentiment = "positive"
            elif message.value['sentiment'] == '0':
                sentiment = "neutral"
            elif message.value['sentiment'] == '-1':
                sentiment = "negative"
            
            with lock:
                sentiment_cnt.update({sentiment: 1})
            print("[SENTIMENTS] "+ ", ".join(f"{key}: {value}" for key, value in sentiment_cnt.items()))

            with lock:
                sentiment_i += 1
        print(f"Processed langs {lang_i} messages")
        print(f"Processed keywords {keyword_i} messages")
        print(f"Processed sentiment {sentiment_i} messages")

        consumer.commit()

    consumer.close()

def main():
    load_dotenv()

    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")

    topics = ["langs", "keywords", "sentiments"]

    threads = []
    for topic in topics:
        for i in range(0, 10):
            t = threading.Thread(
                                    target=consume,
                                    args=(KAFKA_HOST, KAFKA_PORT, topic,)
                                )
            threads.append(t)
            t.start()
    for t in threads:
        t.join()

    keyword_cnt_top = sorted(keyword_cnt.items(), key=lambda x: x[1], reverse=True)[:10]
    print("=================FINAL STATS========================")
    print(f"Processed langs {lang_i} messages")
    print(f"Processed keywords {keyword_i} messages")
    print(f"Processed sentiment {sentiment_i} messages")
    print("")
    print("[LANGUAGES] "+ ", ".join(f"{key}: {value}" for key, value in lang_cnt.items()))
    print("[SENTIMENTS] "+ ", ".join(f"{key}: {value}" for key, value in sentiment_cnt.items()))
    print("[KEYWORDS] "+ ", ".join(str(f"{x[0]}: {x[1]}") for x in keyword_cnt_top))
    print("====================================================")

if __name__ == '__main__':
    main()