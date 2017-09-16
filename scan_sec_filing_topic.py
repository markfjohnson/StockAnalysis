from kafka import KafkaConsumer
from pyspark.sql import *

__all__ = ["SparkSession"]

spark = SparkSession.builder \
    .master("local") \
    .appName("Read SEC XBRL Kafka references to SEC filings") \
    .getOrCreate()

sc = spark.sparkContext


def analyze_and_save_filing(new_filing):
    print("-------------")
    print (new_filing)
    print ("ABC"+"EFG")


def process_sec_filings():
    kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"

    consumer = KafkaConsumer(bootstrap_servers=kafka_url,auto_offset_reset='earliest')
    consumer.subscribe(['sec_filing'])
    print("Consumer length")
    print("Completed subscription")

    while True :
        filings = sc.parallelize(consumer.poll(50))
        print("Processing {} new filings",filings.count())
        filings.map( lambda f: analyze_and_save_filing(f))



if __name__ == "__main__":
    print("Start scanner")
    process_sec_filings()
    print("End scanner")