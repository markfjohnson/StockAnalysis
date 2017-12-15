import os
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import sys
sys.path.append('/opt/spark/dist/jars')
import json

__all__ = ["SparkSession"]

kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"
kafka_url = "localhost:9092"
batch_size = 5
def setup_spark_streaming_receiver():
    sc = SparkContext(appName="SEC_Filings_Streaming_Processor")
    sc.setLogLevel("DEBUG")
    ssc = StreamingContext(sc, batch_size)

    # Define Kafka Consumer
    kafka_stream = KafkaUtils.createStream(ssc, kafka_url, 'sec_ingester', {'sec_filing':1})

    ## --- Processing
    # Extract SEC Filings
    #parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    # Count number of tweets in the batch
    count_this_batch = kafka_stream.count().map(lambda x:('SEC filings this batch: %s' % x))

    # Count by windowed time period
    count_windowed = kafka_stream.countByWindow(60,5).map(lambda x:('Filings total (One minute rolling count): %s' % x))



    # Write tweet author counts to stdout
    count_this_batch.pprint(5)
    count_windowed.pprint(5)

    return ssc

if __name__ == "__main__":
    setup_spark_streaming_receiver()