
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

def get_value(refEntity, key):
    print("Start of get value-really I meanit")
    res=None
    try:
        res = refEntity[key]
    finally:
        print("Result="+res)
    return(res)

def process_SEC_rss(year, month):
    producer = KafkaProducer(bootstrap_servers=kafka_url)
    index_rss = 'http://www.sec.gov/Archives/edgar/monthly/xbrlrss-' + str(year) + '-' + str(month).zfill(2) + '.xml'

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
    #        xbrlFiles = filingInfo['edgar:xbrlFiles']['edgar:xbrlFile']
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
            print("-----------------")
            producer.send('sec_filing',json.dumps(newRow))


if __name__ == "__main__":
    process_SEC_rss(2017,8)

