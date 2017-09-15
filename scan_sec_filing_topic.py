from kafka import KafkaConsumer
from pyspark.sql import *

__all__ = ["SparkSession"]

spark = SparkSession.builder \
    .master("local") \
    .appName("Read SEC XBRL Kafka references to SEC filings") \
    .getOrCreate()

sc = spark.sparkContext

def analyze_and_save_filing(new_filing):
    print (new_filing)
    print ("ABC"+"EFG")

def process_sec_filings():
    kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"

    consumer = KafkaConsumer(bootstrap_servers=kafka_url,auto_offset_reset='earliest')
    consumer.subscribe(['sec_filing'])

    current_filings = []
    for message in consumer:
        current_filings.append(message)

    found_messages = len(current_filings)
    print("Found {} new filings.", found_messages)

    if (found_messages > 0):
        filings = sc.parallelize(current_filings)
        processed_filings = filings.map( lambda f: analyze_and_save_filing(f))
        print(processed_filings)



if __name__ == "__main__":
    process_sec_filings()