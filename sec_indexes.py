import urllib2
import xmltodict
import json
# import psycopg2
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# sample program

from kafka import KafkaProducer
from pyspark.sql import *

__all__ = ["SparkSession"]

cur = None

kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"
#kafka_url = "localhost:9092"

topic_name = "sec_filings"
print("Starting Push filings")
spark = SparkSession.builder \
    .master("local") \
    .config("spark.submit.deployMode","cluster") \
    .appName("Read SEC XBRL RSS files into Kafka") \
    .getOrCreate()
sc = spark.sparkContext


def get_value(refEntity, key):
    res = None
    try:
        res = refEntity[key]
    finally:
        return (res)


def process_SEC_rss(item):
    # print("processing SEC filing for: {}", item)
    # print("test it and again")
    producer = KafkaProducer(bootstrap_servers=kafka_url)
    index_rss = 'http://www.sec.gov/Archives/edgar/monthly/xbrlrss-{}.xml'.format(item)
    producer = KafkaProducer(bootstrap_servers=kafka_url)
    rss_feed = urllib2.urlopen(index_rss)
    index_data = rss_feed.read()
    rss_feed.close()
    index_doc = xmltodict.parse(index_data)
    item_list = index_doc['rss']['channel']['item']
    msg_count = 0
    for entry in item_list:
        formType = entry['edgar:xbrlFiling']['edgar:formType']
        filingInfo = entry['edgar:xbrlFiling']

        if (formType == '10-Q' or formType == '10-K'):
            newRow = {
                'companyName': get_value(filingInfo, 'edgar:companyName'),
                'guid': get_value(entry, 'guid'),
                'xml_filing': index_rss,
                'pubDate': get_value(entry, 'pubDate'),
                'formType': formType,
                'filingDate': get_value(filingInfo, 'edgar:filingDate'),
                'cikNumbver': get_value(filingInfo, 'edgar:cikNumber'),
                'accessionNumber': get_value(filingInfo, 'edgar:accessionNumber'),
                'fileNumber': get_value(filingInfo, 'edgar:fileNumber'),
                'filingInfo': get_value(filingInfo, 'edgar:period'),
                'fiscalYearEnd': get_value(filingInfo, 'edgar:fiscalYearEnd'),
            }
            jsec = json.dumps(newRow)
            producer.send(topic_name, jsec)
            producer.flush()

            msg_count = msg_count + 1

    producer.close()


def build_processing_list():
    process_list = []
    for year in range(2010, 2017):
        for month in range(1, 12):
            process_list.append("{}-{}".format(year, str(month).zfill(2)))
    print("Built the processing list: {}".format(len(process_list)))
    return (process_list)


def test_map_output(x):
    return x


def bulk_process_months():
    b = None
    try:
        s = build_processing_list()
        print("Built the list {}".format(s))
        process_list = sc.parallelize(s)
        print("PARALLELIZED THE LIST")
        b = process_list.map(lambda x: process_SEC_rss(x))
        print("result ")
        for x in b.collect():
            print x
    except Exception as e:
        print(e)
    finally:
        print("Trully finished the map")


if __name__ == "__main__":
    bulk_process_months()
#     process_SEC_rss("2017-08")
