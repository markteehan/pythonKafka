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
#REGEXP_GDELTEVENT = RE.COMPILE("(?P<EVENTID>.*?)\t(?P<DAY>.*?)\t(?P<MONTHYEAR>.*?)\t(?P<YEAR>.*?)\t(?P<FRACTIONDATE>.*?)\t(?P<ACTOR1CODE>.*?)\t(?P<ACTOR1NAME>.*?)\t(?P<ACTOR1COUNTRYCODE>.*?)\t(?P<ACTOR1KNOWNGROUPCODE>.*?)\t(?P<ACTOR1ETHNICCODE>.*?)\t(?P<ACTOR1RELIGION1CODE>.*?)\t(?P<ACTOR1RELIGION2CODE>.*?)\t(?P<ACTOR1TYPE1CODE>.*?)\t(?P<ACTOR1TYPE2CODE>.*?)\t(?P<ACTOR1TYPE3CODE>.*?)\t(?P<ACTOR2CODE>.*?)\t(?P<ACTOR2NAME>.*?)\t(?P<ACTOR2COUNTRYCODE>.*?)\t(?P<ACTOR2KNOWNGROUPCODE>.*?)\t(?P<ACTOR2ETHNICCODE>.*?)\t(?P<ACTOR2RELIGION1CODE>.*?)\t(?P<ACTOR2RELIGION2CODE>.*?)\t(?P<ACTOR2TYPE1CODE>.*?)\t(?P<ACTOR2TYPE2CODE>.*?)\t(?P<ACTOR2TYPE3CODE>.*?)\t(?P<ISROOTEVENT>.*?)\t(?P<EVENTCODE>.*?)\t(?P<EVENTBASECODE>.*?)\t(?P<EVENTROOTCODE>.*?)\t(?P<QUADCLASS>.*?)\t(?P<GOLDSTEINSCALE>.*?)\t(?P<NUMMENTIONS>.*?)\t(?P<NUMSOURCES>.*?)\t(?P<NUMARTICLES>.*?)\t(?P<AVGTONE>.*?)\t(?P<ACTOR1GEO_TYPE>.*?)\t(?P<ACTOR1GEO_FULLNAME>.*?)\t(?P<ACTOR1GEO_COUNTRYCODE>.*?)\t(?P<ACTOR1GEO_ADM1CODE>.*?)\t(?P<ACTOR1GEO_ADM2CODE>.*?)\t(?P<ACTOR1GEO_LAT>.*?)\t(?P<ACTOR1GEO_LONG>.*?)\t(?P<ACTOR1GEO_FEATUREID>.*?)\t(?P<ACTOR2GEO_TYPE>.*?)\t(?P<ACTOR2GEO_FULLNAME>.*?)\t(?P<ACTOR2GEO_COUNTRYCODE>.*?)\t(?P<ACTOR2GEO_ADM1CODE>.*?)\t(?P<ACTOR2GEO_ADM2CODE>.*?)\t(?P<ACTOR2GEO_LAT>.*?)\t(?P<ACTOR2GEO_LONG>.*?)\t(?P<ACTOR2GEO_FEATUREID>.*?)\t(?P<ACTIONGEO_TYPE>.*?)\t(?P<ACTIONGEO_FULLNAME>.*?)\t(?P<ACTIONGEO_COUNTRYCODE>.*?)\t(?P<ACTIONGEO_ADM1CODE>.*?)\t(?P<ACTIONGEO_ADM2CODE>.*?)\t(?P<ACTIONGEO_LAT>.*?)\t(?P<ACTIONGEO_LONG>.*?)\t(?P<ACTIONGEO_FEATUREID>.*?)\t(?P<DATEADDED>.*?)\t(?P<SOURCEURL>.*)")
regexp_gdeltEvent = re.compile("(?P<EVENTID>.*?)\t(?P<DAY>.*?)\t(?P<MONTHYEAR>.*?)\t(?P<YEAR>.*?)\t(?P<FRACTIONDATE>.*?)\t(?P<ACTOR1CODE>.*?)\t(?P<ACTOR1NAME>.*?)\t(?P<ACTOR1COUNTRYCODE>.*?)\t(?P<ACTOR1KNOWNGROUPCODE>.*?)\t(?P<ACTOR1ETHNICCODE>.*?)\t(?P<ACTOR1RELIGION1CODE>.*?)\t(?P<ACTOR1RELIGION2CODE>.*?)\t(?P<ACTOR1TYPE1CODE>.*?)\t(?P<ACTOR1TYPE2CODE>.*?)\t(?P<ACTOR1TYPE3CODE>.*?)\t(?P<ACTOR2CODE>.*?)\t(?P<ACTOR2NAME>.*?)\t(?P<ACTOR2COUNTRYCODE>.*?)\t(?P<ACTOR2KNOWNGROUPCODE>.*?)\t(?P<ACTOR2ETHNICCODE>.*?)\t(?P<ACTOR2RELIGION1CODE>.*?)\t(?P<ACTOR2RELIGION2CODE>.*?)\t(?P<ACTOR2TYPE1CODE>.*?)\t(?P<ACTOR2TYPE2CODE>.*?)\t(?P<ACTOR2TYPE3CODE>.*?)\t(?P<ISROOTEVENT>.*?)\t(?P<EVENTCODE>.*?)\t(?P<EVENTBASECODE>.*?)\t(?P<EVENTROOTCODE>.*?)\t(?P<QUADCLASS>.*?)\t(?P<GOLDSTEINSCALE>.*?)\t(?P<NUMMENTIONS>.*?)\t(?P<NUMSOURCES>.*?)\t(?P<NUMARTICLES>.*?)\t(?P<AVGTONE>.*?)\t(?P<ACTOR1GEO_TYPE>.*?)\t(?P<ACTOR1GEO_FULLNAME>.*?)\t(?P<ACTOR1GEO_COUNTRYCODE>.*?)\t(?P<ACTOR1GEO_ADM1CODE>.*?)\t(?P<ACTOR1GEO_ADM2CODE>.*?)\t(?P<ACTOR1GEO_LAT>.*?)\t(?P<ACTOR1GEO_LONG>.*?)\t(?P<ACTOR1GEO_FEATUREID>.*?)\t(?P<ACTOR2GEO_TYPE>.*?)\t(?P<ACTOR2GEO_FULLNAME>.*?)\t(?P<ACTOR2GEO_COUNTRYCODE>.*?)\t(?P<ACTOR2GEO_ADM1CODE>.*?)\t(?P<ACTOR2GEO_ADM2CODE>.*?)\t(?P<ACTOR2GEO_LAT>.*?)\t(?P<ACTOR2GEO_LONG>.*?)\t(?P<ACTOR2GEO_FEATUREID>.*?)\t(?P<ACTIONGEO_TYPE>.*?)\t(?P<ACTIONGEO_FULLNAME>.*?)\t(?P<ACTIONGEO_COUNTRYCODE>.*?)\t(?P<ACTIONGEO_ADM1CODE>.*?)\t(?P<ACTIONGEO_ADM2CODE>.*?)\t(?P<ACTIONGEO_LAT>.*?)\t(?P<ACTIONGEO_LONG>.*?)\t(?P<ACTIONGEO_FEATUREID>.*?)\t(?P<DATEADDED>.*?)\t(?P<SOURCEURL>.*)")
# Match any character until space [^\s]+


def Produce_gdeltEvent(topic,data,key):
  avroProducer_gdeltEvent.produce(topic=topic, value=data,key=key)


def load(datafile, topic, server):
    global count_gdeltEvent
    global avroProducer_gdeltEvent
    flusher=0
    with open(datafile) as f:
      for row in f.readlines():
          if not re.match(regexp_gdeltEvent, str(row)):
              print("(E) No Match for regexp_gdeltEvent!")
          if     re.match(regexp_gdeltEvent, str(row)):
              match=re.match(regexp_gdeltEvent,row)
              count_gdeltEvent+=1

              key = {
             'EVENTID': match.group('EVENTID')
              };

              key = {
                  'MONTHYEAR': match.group('MONTHYEAR')
              };

              data = {
                    'EVENTID': match.group('EVENTID')
                  , 'DAY': match.group('DAY')
                  , 'MONTHYEAR': match.group('MONTHYEAR')
                  , 'YEAR': match.group('YEAR')
                  , 'FRACTIONDATE': match.group('FRACTIONDATE')
                  , 'ACTOR1CODE': match.group('ACTOR1CODE')
                  , 'ACTOR1NAME': match.group('ACTOR1NAME')
                  , 'ACTOR1COUNTRYCODE': match.group('ACTOR1COUNTRYCODE')
                  , 'ACTOR1KNOWNGROUPCODE': match.group('ACTOR1KNOWNGROUPCODE')
                  , 'ACTOR1ETHNICCODE': match.group('ACTOR1ETHNICCODE')
                  , 'ACTOR1RELIGION1CODE': match.group('ACTOR1RELIGION1CODE')
                  , 'ACTOR1RELIGION2CODE': match.group('ACTOR1RELIGION2CODE')
                  , 'ACTOR1TYPE1CODE': match.group('ACTOR1TYPE1CODE')
                  , 'ACTOR1TYPE2CODE': match.group('ACTOR1TYPE2CODE')
                  , 'ACTOR1TYPE3CODE': match.group('ACTOR1TYPE3CODE')
                  , 'ACTOR2CODE': match.group('ACTOR2CODE')
                  , 'ACTOR2NAME': match.group('ACTOR2NAME')
                  , 'ACTOR2COUNTRYCODE': match.group('ACTOR2COUNTRYCODE')
                  , 'ACTOR2KNOWNGROUPCODE': match.group('ACTOR2KNOWNGROUPCODE')
                  , 'ACTOR2ETHNICCODE': match.group('ACTOR2ETHNICCODE')
                  , 'ACTOR2RELIGION1CODE': match.group('ACTOR2RELIGION1CODE')
                  , 'ACTOR2RELIGION2CODE': match.group('ACTOR2RELIGION2CODE')
                  , 'ACTOR2TYPE1CODE': match.group('ACTOR2TYPE1CODE')
                  , 'ACTOR2TYPE2CODE': match.group('ACTOR2TYPE2CODE')
                  , 'ACTOR2TYPE3CODE': match.group('ACTOR2TYPE3CODE')
                  , 'ISROOTEVENT': match.group('ISROOTEVENT')
                  , 'EVENTCODE': match.group('EVENTCODE')
                  , 'EVENTBASECODE': match.group('EVENTBASECODE')
                  , 'EVENTROOTCODE': match.group('EVENTROOTCODE')
                  , 'QUADCLASS': match.group('QUADCLASS')
                  , 'GOLDSTEINSCALE': match.group('GOLDSTEINSCALE')
                  , 'NUMMENTIONS': match.group('NUMMENTIONS')
                  , 'NUMSOURCES': match.group('NUMSOURCES')
                  , 'NUMARTICLES': match.group('NUMARTICLES')
                  , 'AVGTONE': match.group('AVGTONE')
                  , 'ACTOR1GEO_TYPE': match.group('ACTOR1GEO_TYPE')
                  , 'ACTOR1GEO_FULLNAME': match.group('ACTOR1GEO_FULLNAME')
                  , 'ACTOR1GEO_COUNTRYCODE': match.group('ACTOR1GEO_COUNTRYCODE')
                  , 'ACTOR1GEO_ADM1CODE': match.group('ACTOR1GEO_ADM1CODE')
                  , 'ACTOR1GEO_ADM2CODE': match.group('ACTOR1GEO_ADM2CODE')
                  , 'ACTOR1GEO_LAT': match.group('ACTOR1GEO_LAT')
                  , 'ACTOR1GEO_LONG': match.group('ACTOR1GEO_LONG')
                  , 'ACTOR1GEO_FEATUREID': match.group('ACTOR1GEO_FEATUREID')
                  , 'ACTOR2GEO_TYPE': match.group('ACTOR2GEO_TYPE')
                  , 'ACTOR2GEO_FULLNAME': match.group('ACTOR2GEO_FULLNAME')
                  , 'ACTOR2GEO_COUNTRYCODE': match.group('ACTOR2GEO_COUNTRYCODE')
                  , 'ACTOR2GEO_ADM1CODE': match.group('ACTOR2GEO_ADM1CODE')
                  , 'ACTOR2GEO_LAT': match.group('ACTOR2GEO_LAT')
                  , 'ACTOR2GEO_LONG': match.group('ACTOR2GEO_LONG')
                  , 'ACTOR2GEO_FEATUREID': match.group('ACTOR2GEO_FEATUREID')
                  , 'ACTIONGEO_TYPE': match.group('ACTIONGEO_TYPE')
                  , 'ACTIONGEO_FULLNAME': match.group('ACTIONGEO_FULLNAME')
                  , 'ACTIONGEO_COUNTRYCODE': match.group('ACTIONGEO_COUNTRYCODE')
                  , 'ACTIONGEO_ADM1CODE': match.group('ACTIONGEO_ADM1CODE')
                  , 'ACTIONGEO_LAT': match.group('ACTIONGEO_LAT')
                  , 'ACTIONGEO_LONG': match.group('ACTIONGEO_LONG')
                  , 'ACTIONGEO_FEATUREID': match.group('ACTIONGEO_FEATUREID')
                  , 'DATEADDED': match.group('DATEADDED')
                  , 'SOURCEURL': match.group('SOURCEURL')
                  , 'SITE': match.group('SOURCEURL').split("/")[2]
              }
              vStrTopic='{"'+topic+'":"'+str(match.group('EVENTID'))+'"}'
              #print(" producing topic for"+vStrTopic)
              avroProducer_gdeltEvent.produce(topic=topic, value=data, key=key)
              flusher = flusher + 1
              # "commit" every 100 messages. Otherwise its slow as a dog
              if flusher == 100:
                avroProducer_gdeltEvent.flush()
                flusher=0

    print("Loaded topic "+topic+":"+str(count_gdeltEvent))

count_gdeltEvent = 0
#{"name": "EVENTID" ,"type": ["null","string"],"default":null}
key_schema = """
{"namespace": "gdelt.event.avro",
      "type": "record",
      "name": "REPLACEME_TOPIC_key",
    "fields": [
               {"name": "MONTHYEAR" ,"type": ["null","string"],"default":null}               
              ]
}""".replace("REPLACEME_TOPIC", topic);


schema_values_str = """
{"namespace": "gdelt.event.avro",
"type": "record",
"name": "REPLACEME_TOPIC",
"fields": [
{ "name": "EVENTID" ,"type": ["null","string"],"default":null}
,{"name": "DAY" ,"type": ["null","string"],"default":null}
,{"name": "MONTHYEAR" ,"type": ["null","string"],"default":null}
,{"name": "YEAR" ,"type": ["null","string"],"default":null}
,{"name": "FRACTIONDATE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1NAME" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1COUNTRYCODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1KNOWNGROUPCODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1ETHNICCODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1RELIGION1CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1RELIGION2CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1TYPE1CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1TYPE2CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1TYPE3CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2NAME" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2COUNTRYCODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2KNOWNGROUPCODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2ETHNICCODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2RELIGION1CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2RELIGION2CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2TYPE1CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2TYPE2CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2TYPE3CODE" ,"type": ["null","string"],"default":null}
,{"name": "ISROOTEVENT" ,"type": ["null","string"],"default":null}
,{"name": "EVENTCODE" ,"type": ["null","string"],"default":null}
,{"name": "EVENTBASECODE" ,"type": ["null","string"],"default":null}
,{"name": "EVENTROOTCODE" ,"type": ["null","string"],"default":null}
,{"name": "QUADCLASS" ,"type": ["null","string"],"DEFAULET":null}
,{"name": "GOLDSTEINSCALE" ,"type": ["null","string"],"default":null}
,{"name": "NUMMENTIONS" ,"type": ["null","string"],"default":null}
,{"name": "NUMSOURCES" ,"type": ["null","string"],"default":null}
,{"name": "NUMARTICLES" ,"type": ["null","string"],"default":null}
,{"name": "AVGTONE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1GEO_TYPE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1GEO_FULLNAME" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1GEO_COUNTRYCODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1GEO_ADM1CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1GEO_ADM2CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1GEO_LAT" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1GEO_LONG" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR1GEO_FEATUREID" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2GEO_TYPE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2GEO_FULLNAME" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2GEO_COUNTRYCODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2GEO_ADM1CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2GEO_ADM2CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2GEO_LAT" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2GEO_LONG" ,"type": ["null","string"],"default":null}
,{"name": "ACTOR2GEO_FEATUREID" ,"type": ["null","string"],"default":null}
,{"name": "ACTIONGEO_TYPE" ,"type": ["null","string"],"default":null}
,{"name": "ACTIONGEO_FULLNAME" ,"type": ["null","string"],"default":null}
,{"name": "ACTIONGEO_COUNTRYCODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTIONGEO_ADM1CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTIONGEO_ADM2CODE" ,"type": ["null","string"],"default":null}
,{"name": "ACTIONGEO_LAT" ,"type": ["null","string"],"default":null}
,{"name": "ACTIONGEO_LONG" ,"type": ["null","string"],"default":null}
,{"name": "ACTIONGEO_FEATUREID" ,"type": ["null","string"],"default":null}
,{"name": "DATEADDED" ,"type": ["null","string"],"default":null}
,{"name": "SOURCEURL" ,"type": ["null","string"],"default":null}
,{"name": "SITE" ,"type": ["null","string"],"default":null}
]
}""".replace("REPLACEME_TOPIC", topic)

key_schema   = avro.loads(key_schema)
value_schema = avro.loads(schema_values_str)


avroProducer_gdeltEvent = AvroProducer({'bootstrap.servers': server
                                      , 'schema.registry.url': schema_registry_url}
                                      , default_value_schema=value_schema
                                      ,   default_key_schema=key_schema
                                    )
load(datafile, topic, server)
avroProducer_gdeltEvent.flush()

#if __name__ == "__main__":
#    main()