import sys
import csv
import codecs
import re

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
#hostname='marks-MacBook.local'
count_gdeltEvente=0
datafile = ''
schema = ''
topic = ""
hostname = ""


if len(sys.argv) > 1:
     topic = sys.argv[1]
if len(sys.argv) > 2:
     datafile = sys.argv[2]
if len(sys.argv) > 3:
     hostname = sys.argv[3]

server = hostname+':9092'
print("(I) processing for topic "+topic+" and file "+datafile +" and hostname "+hostname)

server = hostname+':9092'



#
# The next two lines prevent errose from unicode characters:
# UnicodeDecodeErroer: 'ascii' codec can't decode byte 0xc2 in position 16: ordinal not in range(128)
reload(sys)
sys.setdefaultencoding('utf8')

# 23-jun-2018 MT add GDELT
countProduced=0
schema_registry_url = 'http://'+hostname+':8081'
regexp_gdeltEvent = re.compile("(?P<EventId>.*?)\t(?P<Day>.*?)\t(?P<MonthYear>.*?)\t(?P<Year>.*?)\t(?P<FractionDate>.*?)\t(?P<Actor1Code>.*?)\t(?P<Actor1Name>.*?)\t(?P<Actor1CountryCode>.*?)\t(?P<Actor1KnownGroupCode>.*?)\t(?P<Actor1EthnicCode>.*?)\t(?P<Actor1Religion1Code>.*?)\t(?P<Actor1Religion2Code>.*?)\t(?P<Actor1Type1Code>.*?)\t(?P<Actor1Type2Code>.*?)\t(?P<Actor1Type3Code>.*?)\t(?P<Actor2Code>.*?)\t(?P<Actor2Name>.*?)\t(?P<Actor2CountryCode>.*?)\t(?P<Actor2KnownGroupCode>.*?)\t(?P<Actor2EthnicCode>.*?)\t(?P<Actor2Religion1Code>.*?)\t(?P<Actor2Religion2Code>.*?)\t(?P<Actor2Type1Code>.*?)\t(?P<Actor2Type2Code>.*?)\t(?P<Actor2Type3Code>.*?)\t(?P<IsRootEvent>.*?)\t(?P<EventCode>.*?)\t(?P<EventBaseCode>.*?)\t(?P<EventRootCode>.*?)\t(?P<QuadClass>.*?)\t(?P<GoldsteinScale>.*?)\t(?P<NumMentions>.*?)\t(?P<NumSources>.*?)\t(?P<NumArticles>.*?)\t(?P<AvgTone>.*?)\t(?P<Actor1Geo_Type>.*?)\t(?P<Actor1Geo_FullName>.*?)\t(?P<Actor1Geo_CountryCode>.*?)\t(?P<Actor1Geo_ADM1Code>.*?)\t(?P<Actor1Geo_ADM2Code>.*?)\t(?P<Actor1Geo_Lat>.*?)\t(?P<Actor1Geo_Long>.*?)\t(?P<Actor1Geo_FeatureID>.*?)\t(?P<Actor2Geo_Type>.*?)\t(?P<Actor2Geo_FullName>.*?)\t(?P<Actor2Geo_CountryCode>.*?)\t(?P<Actor2Geo_ADM1Code>.*?)\t(?P<Actor2Geo_ADM2Code>.*?)\t(?P<Actor2Geo_Lat>.*?)\t(?P<Actor2Geo_Long>.*?)\t(?P<Actor2Geo_FeatureID>.*?)\t(?P<ActionGeo_Type>.*?)\t(?P<ActionGeo_FullName>.*?)\t(?P<ActionGeo_CountryCode>.*?)\t(?P<ActionGeo_ADM1Code>.*?)\t(?P<ActionGeo_ADM2Code>.*?)\t(?P<ActionGeo_Lat>.*?)\t(?P<ActionGeo_Long>.*?)\t(?P<ActionGeo_FeatureID>.*?)\t(?P<DateAdded>.*?)\t(?P<SourceUrl>.*)")
# Match any character until space [^\s]+


def Produce_gdeltEvent(topic,data,key):
  avroProducer_gdeltEvent.produce(topic=topic, value=data)


def load(datafile, topic, server):
    global count_gdeltEvent
    global avroProducer_gdeltEvent
    with open(datafile) as f:
      for row in f.readlines():
          if not re.match(regexp_gdeltEvent, str(row)):
              print("(E) No Match for regexp_gdeltEvent!")
          if     re.match(regexp_gdeltEvent, str(row)):
              match=re.match(regexp_gdeltEvent,row)
              count_gdeltEvent+=1
              data = {
              'EventId':match.group('EventId')
              , 'Day':match.group('Day')
              , 'MonthYear':match.group('MonthYear')
              , 'Year':match.group('Year')
              , 'FractionDate':match.group('FractionDate')
              , 'Actor1Code':match.group('Actor1Code')
              , 'Actor1Name':match.group('Actor1Name')
              , 'Actor1CountryCode':match.group('Actor1CountryCode')
              , 'Actor1KnownGroupCode':match.group('Actor1KnownGroupCode')
              , 'Actor1EthnicCode':match.group('Actor1EthnicCode')
              , 'Actor1Religion1Code':match.group('Actor1Religion1Code')
              , 'Actor1Religion2Code':match.group('Actor1Religion2Code')
              , 'Actor1Type1Code':match.group('Actor1Type1Code')
              , 'Actor1Type2Code':match.group('Actor1Type2Code')
              , 'Actor1Type3Code':match.group('Actor1Type3Code')
              , 'Actor2Code':match.group('Actor2Code')
              , 'Actor2Name':match.group('Actor2Name')
              , 'Actor2CountryCode':match.group('Actor2CountryCode')
              , 'Actor2KnownGroupCode':match.group('Actor2KnownGroupCode')
              , 'Actor2EthnicCode':match.group('Actor2EthnicCode')
              , 'Actor2Religion1Code':match.group('Actor2Religion1Code')
              , 'Actor2Religion2Code':match.group('Actor2Religion2Code')
              , 'Actor2Type1Code':match.group('Actor2Type1Code')
              , 'Actor2Type2Code':match.group('Actor2Type2Code')
              , 'Actor2Type3Code':match.group('Actor2Type3Code')
              , 'IsRootEvent':match.group('IsRootEvent')
              , 'EventCode':match.group('EventCode')
              , 'EventBaseCode':match.group('EventBaseCode')
              , 'EventRootCode':match.group('EventRootCode')
              , 'QuadClass':match.group('QuadClass')
              , 'GoldsteinScale':match.group('GoldsteinScale')
              , 'NumMentions':match.group('NumMentions')
              , 'NumSources':match.group('NumSources')
              , 'NumArticles':match.group('NumArticles')
              , 'AvgTone':match.group('AvgTone')
              , 'Actor1Geo_Type':match.group('Actor1Geo_Type')
              , 'Actor1Geo_FullName':match.group('Actor1Geo_FullName')
              , 'Actor1Geo_CountryCode':match.group('Actor1Geo_CountryCode')
              , 'Actor1Geo_ADM1Code':match.group('Actor1Geo_ADM1Code')
              , 'Actor1Geo_ADM2Code':match.group('Actor1Geo_ADM2Code')
              , 'Actor1Geo_Lat':match.group('Actor1Geo_Lat')
              , 'Actor1Geo_Long':match.group('Actor1Geo_Long')
              , 'Actor1Geo_FeatureID':match.group('Actor1Geo_FeatureID')
              , 'Actor2Geo_Type':match.group('Actor2Geo_Type')
              , 'Actor2Geo_FullName':match.group('Actor2Geo_FullName')
              , 'Actor2Geo_CountryCode':match.group('Actor2Geo_CountryCode')
              , 'Actor2Geo_ADM1Code':match.group('Actor2Geo_ADM1Code')
              , 'Actor2Geo_Lat':match.group('Actor2Geo_Lat')
              , 'Actor2Geo_Long':match.group('Actor2Geo_Long')
              , 'Actor2Geo_FeatureID':match.group('Actor2Geo_FeatureID')
              , 'ActionGeo_Type':match.group('ActionGeo_Type')
              , 'ActionGeo_FullName':match.group('ActionGeo_FullName')
              , 'ActionGeo_CountryCode':match.group('ActionGeo_CountryCode')
              , 'ActionGeo_ADM1Code':match.group('ActionGeo_ADM1Code')
              , 'ActionGeo_Lat':match.group('ActionGeo_Lat')
              , 'ActionGeo_Long':match.group('ActionGeo_Long')
              , 'ActionGeo_FeatureID':match.group('ActionGeo_FeatureID')
              , 'DateAdded':match.group('DateAdded')
              , 'SourceUrl':match.group('SourceUrl')
              , 'Site': match.group('SourceUrl').split("/")[2]
              }
              vStrTopic='{"'+topic+'":"'+str(match.group('EventId'))+'"}'
              #print(" producing topic for"+vStrTopic)
              avroProducer_gdeltEvent.produce(topic=topic, value=data)
    avroProducer_gdeltEvent.flush()
    print("Loaded topic "+topic+":"+str(count_gdeltEvent))

#def main():
 # my code here
#setupTopic_gdeltEvent(server, schema_registry_url,topic)
count_gdeltEvent = 0

schema_values_str = """
{"namespace": "gdelt.event.avro",
"type": "record",
"name": "REPLACEME_TOPIC",
"fields": [
{"name": "EventId" ,"type": ["null","string"],"default":null}
,{"name": "Day" ,"type": ["null","string"],"default":null}
,{"name": "MonthYear" ,"type": ["null","string"],"default":null}
,{"name": "Year" ,"type": ["null","string"],"default":null}
,{"name": "FractionDate" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Name" ,"type": ["null","string"],"default":null}
,{"name": "Actor1CountryCode" ,"type": ["null","string"],"default":null}
,{"name": "Actor1KnownGroupCode" ,"type": ["null","string"],"default":null}
,{"name": "Actor1EthnicCode" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Religion1Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Religion2Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Type1Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Type2Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Type3Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Name" ,"type": ["null","string"],"default":null}
,{"name": "Actor2CountryCode" ,"type": ["null","string"],"default":null}
,{"name": "Actor2KnownGroupCode" ,"type": ["null","string"],"default":null}
,{"name": "Actor2EthnicCode" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Religion1Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Religion2Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Type1Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Type2Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Type3Code" ,"type": ["null","string"],"default":null}
,{"name": "IsRootEvent" ,"type": ["null","string"],"default":null}
,{"name": "EventCode" ,"type": ["null","string"],"default":null}
,{"name": "EventBaseCode" ,"type": ["null","string"],"default":null}
,{"name": "EventRootCode" ,"type": ["null","string"],"default":null}
,{"name": "QuadClass" ,"type": ["null","string"],"defaulet":null}
,{"name": "GoldsteinScale" ,"type": ["null","string"],"default":null}
,{"name": "NumMentions" ,"type": ["null","string"],"default":null}
,{"name": "NumSources" ,"type": ["null","string"],"default":null}
,{"name": "NumArticles" ,"type": ["null","string"],"default":null}
,{"name": "AvgTone" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Geo_Type" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Geo_FullName" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Geo_CountryCode" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Geo_ADM1Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Geo_ADM2Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Geo_Lat" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Geo_Long" ,"type": ["null","string"],"default":null}
,{"name": "Actor1Geo_FeatureID" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Geo_Type" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Geo_FullName" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Geo_CountryCode" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Geo_ADM1Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Geo_ADM2Code" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Geo_Lat" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Geo_Long" ,"type": ["null","string"],"default":null}
,{"name": "Actor2Geo_FeatureID" ,"type": ["null","string"],"default":null}
,{"name": "ActionGeo_Type" ,"type": ["null","string"],"default":null}
,{"name": "ActionGeo_FullName" ,"type": ["null","string"],"default":null}
,{"name": "ActionGeo_CountryCode" ,"type": ["null","string"],"default":null}
,{"name": "ActionGeo_ADM1Code" ,"type": ["null","string"],"default":null}
,{"name": "ActionGeo_ADM2Code" ,"type": ["null","string"],"default":null}
,{"name": "ActionGeo_Lat" ,"type": ["null","string"],"default":null}
,{"name": "ActionGeo_Long" ,"type": ["null","string"],"default":null}
,{"name": "ActionGeo_FeatureID" ,"type": ["null","string"],"default":null}
,{"name": "DateAdded" ,"type": ["null","string"],"default":null}
,{"name": "SourceUrl" ,"type": ["null","string"],"default":null}
,{"name": "Site" ,"type": ["null","string"],"default":null}
]
}""".replace("REPLACEME_TOPIC", topic)

value_schema = avro.loads(schema_values_str)


avroProducer_gdeltEvent = AvroProducer({'bootstrap.servers': server
                                      , 'schema.registry.url': schema_registry_url}
                                      , default_value_schema=value_schema
                                    )
load(datafile, topic, server)
avroProducer_gdeltEvent.flush()

#if __name__ == "__main__":
#    main()