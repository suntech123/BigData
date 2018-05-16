#!/usr/bin/python-latest/bin/python3.6
"""
 Consumes messages from a kafka topic
 Usage: ./simple_topic_based_consumer.py
"""
from kafka import KafkaConsumer
consumer = KafkaConsumer('test')
for msg in consumer:
    print(msg)
