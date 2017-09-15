import xmltodict
import json

# sample program

from kafka import KafkaConsumer
from pyspark.sql import *

__all__ = ["SparkSession"]


#kafka_url = "api.kafka.marathon.l4lb.thisdcos.directory:80"
kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"
spark = SparkSession.builder \
            .master("local") \
            .appName("Read SEC XBRL RSS files into Kafka") \
            .getOrCreate()

consumer = KafkaConsumer(bootstrap_servers=kafka_url,
                                 auto_offset_reset='earliest')
        consumer.subscribe(['sec_filing'])

        for message in consumer:
            print (message)