
import urllib2
import xmltodict

from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import *

__all__ = ["SparkSession"]


#kafka_url = "api.kafka.marathon.l4lb.thisdcos.directory:80"
kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"
spark = SparkSession.builder \
            .master("local") \
            .appName("Read SEC XBRL RSS files into Kafka") \
            .getOrCreate()



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
            newRow['companyName'] = filingInfo['edgar:companyName']
            newRow['guid'] =entry['guid']
            newRow['xml_filing'] =index_rss
            newRow['pubDate'] = entry['pubDate']
            newRow['formType'] = formType
            newRow['filingDate'] = filingInfo['edgar:filingDate']
            newRow['cikNumbver'] = filingInfo['edgar:cikNumber']
            newRow['accessionNumber'] = filingInfo['edgar:accessionNumber']
            newRow['fileNumber'] = filingInfo['edgar:fileNumber']
            newRow['filingInfo'] = filingInfo['edgar:period']
            newRow['fiscalYearEnd'] = filingInfo['edgar:fiscalYearEnd']
            print(newRow)
            producer.send('sec_filing',newRow)


if __name__ == "__main__":
    process_SEC_rss(2017,8)

