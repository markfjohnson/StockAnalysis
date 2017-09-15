
import urllib2
import xmltodict
import json

# sample program

from kafka import KafkaProducer
from pyspark.sql import *

__all__ = ["SparkSession"]


#kafka_url = "api.kafka.marathon.l4lb.thisdcos.directory:80"
kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"
spark = SparkSession.builder \
            .master("local") \
            .appName("Read SEC XBRL RSS files into Kafka") \
            .getOrCreate()
sc = spark.sparkContext

def get_value(refEntity, key):
    print("Start of get value")
    res=None
    try:
        res = refEntity[key]
    finally:
        return(res)

def process_SEC_rss(item):

    producer = KafkaProducer(bootstrap_servers=kafka_url)
    index_rss = 'http://www.sec.gov/Archives/edgar/monthly/xbrlrss-{}.xml'.format(item)

    rss_feed = urllib2.urlopen(index_rss)
    index_data = rss_feed.read()
    rss_feed.close()

    index_doc = xmltodict.parse(index_data)
    item_list = index_doc['rss']['channel']['item']

    for entry in item_list:
        formType = entry['edgar:xbrlFiling']['edgar:formType']
        filingInfo = entry['edgar:xbrlFiling']
        print(formType, filingInfo['edgar:companyName'])
        newRow = []
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
            print(newRow)
            producer.send('sec_filing',json.dumps(newRow))


def build_processing_list():
    process_list = []
    for year in range(2000,2017):
        for month in range(1,12):
            process_list.append("{}-{}",year,str(month).zfill(2) )
    return(process_list)


def bulk_process_months():
    process_list = sc.parallelize(build_processing_list())
    process_list.map(lambda mnth: process_list(mnth))



if __name__ == "__main__":
    process_SEC_rss("2017-08")

