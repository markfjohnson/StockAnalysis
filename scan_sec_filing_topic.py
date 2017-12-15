from kafka import KafkaConsumer
from pyspark.sql import *

__all__ = ["SparkSession"]
#kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"
kafka_url = "localhost:9092"
topic_name = "sec_filings"
spark = SparkSession.builder \
            .master("local") \
            .appName("Read SEC XBRL RSS files into Kafka") \
            .getOrCreate()
sc = spark.sparkContext
sc = spark.sparkContext


def analyze_and_save_filing(new_filing):
    print("-------------")
    print (new_filing)
    print ("ABC"+"EFG")


def process_sec_filings():
    consumer = KafkaConsumer(bootstrap_servers=kafka_url,auto_offset_reset='earliest', enable_auto_commit=False, group_id='sec-processor')
    topics = consumer.topics()
    assignments = consumer.assignment()
    metrics = consumer.metrics()
    print metrics
    print assignments
    print topics
    consumer.subscribe(topics)

    while True:
        partitions = consumer.poll(100,100)
        for msg in consumer:
            print msg
        print "---------------------"



if __name__ == "__main__":
    print("Start scanner")
    process_sec_filings()
    print("End scanner")