from pyspark.sql import *

__all__ = ["SparkSession"]

spark = SparkSession.builder \
    .master("mesos://server1:5050") \
    .appName("Read SEC XBRL Kafka references to SEC filings") \
    
    .getOrCreate()

sc = spark.sparkContext
filings = sc.parallelize(range(1,10))
print filings