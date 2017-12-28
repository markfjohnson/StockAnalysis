
import urllib2
import xmltodict
import json
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# sample program

from kafka import KafkaProducer
from pyspark.sql import *

__all__ = ["SparkSession"]

cur = None


#kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"
kafka_url = "localhost:9092"

topic_name = "sec_filings"

spark = SparkSession.builder \
            .master("local") \
            .appName("Read SEC XBRL RSS files into Kafka") \
            .getOrCreate()
sc = spark.sparkContext


def get_value(refEntity, key):
    res=None
    try:
        res = refEntity[key]
    finally:
        return(res)

def process_SEC_rss(item):
<<<<<<< HEAD
=======
    print("processing SEC filing for: {}", item)
    print("test it and again")
    producer = KafkaProducer(bootstrap_servers=kafka_url)
>>>>>>> 3ddd8158bd97d4c9480d231baf31555a033bacc5
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

        if (formType=='10-Q' or formType=='10-K'):
            newRow = {
                'companyName': get_value(filingInfo, 'edgar:companyName'),
                'guid': get_value(entry,'guid'),
                'xml_filing': index_rss,
                'pubDate' : get_value(entry,'pubDate'),
                'formType': formType,
                'filingDate': get_value(filingInfo,'edgar:filingDate'),
                'cikNumbver':  get_value(filingInfo,'edgar:cikNumber'),
                'accessionNumber':  get_value(filingInfo,'edgar:accessionNumber'),
                'fileNumber' :  get_value(filingInfo,'edgar:fileNumber'),
                'filingInfo' : get_value(filingInfo,'edgar:period'),
                'fiscalYearEnd' : get_value(filingInfo,'edgar:fiscalYearEnd'),
            }
#           cols = newRow.keys()
#           vals = [newRow[x] for x in cols]
#            vals_str_list = ["%s"] * len(vals)
#            vals_str = ", ".join(vals_str_list)
            try:
                jsec = json.dumps(newRow)
                producer.send(topic_name, jsec)
                producer.flush()

                msg_count = msg_count + 1

                print("***## Added {} sec filings".format(msg_count))
            except e:
                print "Exception encountered {e}"
    metrics = producer.metrics()
    print metrics
    producer.close()




def build_processing_list():
    process_list = []
    for year in range(2010,2017):
        for month in range(1,12):
            process_list.append("{}-{}".format(year,str(month).zfill(2) ))
    print("Built the processing list: {}".format(len(process_list)))
    return(process_list)


def bulk_process_months():
    s = build_processing_list()
    process_list = sc.parallelize(s)
    process_list.map(lambda x: process_SEC_rss(x))



if __name__ == "__main__":
    bulk_process_months()
#     process_SEC_rss("2017-08")


