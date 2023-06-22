#!/usr/bin/env python3
import os, time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
from dotenv import load_dotenv

def main():
    load_dotenv()

    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    admin_client = KafkaAdminClient(bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"])

    try:
        topic_names = [KAFKA_TOPIC, "langs", "keywords", "sentiments"]
        admin_client.delete_topics(topics=topic_names)
    except UnknownTopicOrPartitionError as e:
        pass

    topic_list = [
        NewTopic(name=KAFKA_TOPIC, num_partitions=10, replication_factor=1),
        NewTopic(name="langs", num_partitions=5, replication_factor=1),
        NewTopic(name="keywords", num_partitions=5, replication_factor=1),
        NewTopic(name="sentiments", num_partitions=5, replication_factor=1)
        ]
    while True:
        try:
            admin_client.create_topics(new_topics=topic_list)
            break
        except TopicAlreadyExistsError as e:
            pass
        time.sleep(1)
        print("Waiting for Kafka to create topics...")

    admin_client.close()

if __name__ == '__main__':
    main()
