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
    print("Completed subscription")

    current_filings = []
    for message in consumer:
        print(message)
    #current_filings.append(message)
    print("A")
    found_messages = len(current_filings)
    print("B")
    print("Found {} new filings.", found_messages)

    if (found_messages > 0):
        print("C")
        filings = sc.parallelize(current_filings)
        print("D")
        processed_filings = filings.map( lambda f: analyze_and_save_filing(f))
        print("E")
        print(processed_filings)



if __name__ == "__main__":
    print("Start scanner")
    process_sec_filings()
    print("End scanner")