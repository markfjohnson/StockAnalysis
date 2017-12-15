
import urllib2
import xmltodict
import json
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# sample program

from kafka import KafkaConsumer
from pyspark.sql import *

__all__ = ["SparkSession"]

cur = None


#kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"
kafka_url = "localhost:9092"

topic_name = "sec_filings"
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=[kafka_url], auto_offset_reset='earliest', group_id=None)
partition = TopicPartition(topic_name, 0)
consumer.assign()
# consumer.topics()
consumer.seek_to_beginning()

for message in consumer:
        print(message)