#!/usr/bin/env python3
import os, csv, json, datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv

def main():
    load_dotenv()
    
    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

if __name__ == '__main__':
    main()
