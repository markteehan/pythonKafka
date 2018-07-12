import sys
import csv
import codecs
import re

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

hostname='marks-MacBook.local'

#
# The next two lines prevent erros from unicode characters:
# UnicodeDecodeError: 'ascii' codec can't decode byte 0xc2 in position 16: ordinal not in range(128)
reload(sys)
sys.setdefaultencoding('utf8')

# 23-jun-2018 MT add GDELT
countProduced=0
schema_registry_url = 'http://'+hostname+':8081'

# Match any character until space [^\s]+

def Produce_gdeltEvent(topic,data,key):
  #avroProducer_gdeltEvent.produce(topic=topic, value=data,key=key)
  avroProducer_gdeltEvent.produce(topic=topic, value=data)

def Produce_routerMessage(topic,data):
  avroProducer_routerMessage.produce(topic=topic, value=data)

def Produce_routerHttp(topic,data):
  avroProducer_routerHttp.produce(topic=topic, value=data)

def Produce_routerHSBridge(topic,data):
  avroProducer_routerHSBridge.produce(topic=topic, value=data)

def Produce_routerElse(topic,data):
  avroProducer_routerElse.produce(topic=topic, value=data)

def setupTopic_gdeltEvent(server,schema_registry_url):
  global regexp_gdeltEvent
  global avroProducer_gdeltEvent
  global count_gdeltEvent
  count_gdeltEvent=0

  # Topic routerMessage scans for a regex that captures this line pattern:

  schema_key_str = """
    {"namespace": "gdelt.event.avro",
    "type": "record",
    "name": "gdeltEvent7_key",
    "fields": [
       {"name": "gdeltEvent7_key" ,"type":"string"}
       ]
    }
  """

  schema_values_str="""
  {"namespace": "gdelt.event.avro",
  "type": "record",
  "name": "gdeltEvent7",
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
    ,{"name": "QuadClass" ,"type": ["null","string"],"default":null}
    ,{"name": "GoldsteinScale" ,"type": ["null","string"],"default":null}
    ,{"name": "NumMentions" ,"type": ["null","string"],"default":null}
    ,{"name": "NumSources" ,"type": ["null","string"],"default":null}
    ,{"name": "NumArticles" ,"type": ["null","string"],"default":null}
    ,{"name": "AvgTone" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor1Geo_Type" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor1Geo_FullName" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor1Geo_CountryCode" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor1Geo_ADM1Code" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor1Geo_Lat" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor1Geo_Long" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor1Geo_FeatureID" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor2Geo_Type" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor2Geo_FullName" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor2Geo_CountryCode" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor2Geo_ADM1Code" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor2Geo_Lat" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor2Geo_Long" ,"type": ["null","string"],"default":null}
    ,{"name": "Actor2Geo_FeatureID" ,"type": ["null","string"],"default":null}
    ,{"name": "ActionGeo_Type" ,"type": ["null","string"],"default":null}
    ,{"name": "ActionGeo_FullName" ,"type": ["null","string"],"default":null}
    ,{"name": "ActionGeo_CountryCode" ,"type": ["null","string"],"default":null}
    ,{"name": "ActionGeo_ADM1Code" ,"type": ["null","string"],"default":null}
    ,{"name": "ActionGeo_Lat" ,"type": ["null","string"],"default":null}
    ,{"name": "ActionGeo_Long" ,"type": ["null","string"],"default":null}
    ,{"name": "ActionGeo_FeatureID" ,"type": ["null","string"],"default":null}
    ,{"name": "DateAdded" ,"type": ["null","string"],"default":null}
    ,{"name": "SourceUrl" ,"type": ["null","string"],"default":null}
  ]
  }"""

  value_schema=avro.loads(schema_values_str)
  key_schema = avro.loads(schema_key_str)

  avroProducer_gdeltEvent = AvroProducer({ 'bootstrap.servers'    : server
                                         , 'schema.registry.url'  : schema_registry_url }
  #                                       , default_key_schema   = key_schema
                                         , default_value_schema = value_schema
                                        )

  regexp_gdeltEvent = re.compile("(?P<EventId>.*?)\t(?P<Day>.*?)\t(?P<MonthYear>.*?)\t(?P<Year>.*?)\t(?P<FractionDate>.*?)\t(?P<Actor1Code>.*?)\t(?P<Actor1Name>.*?)\t(?P<Actor1CountryCode>.*?)\t(?P<Actor1KnownGroupCode>.*?)\t(?P<Actor1EthnicCode>.*?)\t(?P<Actor1Religion1Code>.*?)\t(?P<Actor1Religion2Code>.*?)\t(?P<Actor1Type1Code>.*?)\t(?P<Actor1Type2Code>.*?)\t(?P<Actor1Type3Code>.*?)\t(?P<Actor2Code>.*?)\t(?P<Actor2Name>.*?)\t(?P<Actor2CountryCode>.*?)\t(?P<Actor2KnownGroupCode>.*?)\t(?P<Actor2EthnicCode>.*?)\t(?P<Actor2Religion1Code>.*?)\t(?P<Actor2Religion2Code>.*?)\t(?P<Actor2Type1Code>.*?)\t(?P<Actor2Type2Code>.*?)\t(?P<Actor2Type3Code>.*?)\t(?P<IsRootEvent>.*?)\t(?P<EventCode>.*?)\t(?P<EventBaseCode>.*?)\t(?P<EventRootCode>.*?)\t(?P<QuadClass>.*?)\t(?P<GoldsteinScale>.*?)\t(?P<NumMentions>.*?)\t(?P<NumSources>.*?)\t(?P<NumArticles>.*?)\t(?P<AvgTone>.*?)\t(?P<Actor1Geo_Type>.*?)\t(?P<Actor1Geo_FullName>.*?)\t(?P<Actor1Geo_CountryCode>.*?)\t(?P<Actor1Geo_ADM1Code>.*?)\t(?P<Actor1Geo_Lat>.*?)\t(?P<Actor1Geo_Long>.*?)\t(?P<Actor1Geo_FeatureID>.*?)\t(?P<Actor2Geo_Type>.*?)\t(?P<Actor2Geo_FullName>.*?)\t(?P<Actor2Geo_CountryCode>.*?)\t(?P<Actor2Geo_ADM1Code>.*?)\t(?P<Actor2Geo_Lat>.*?)\t(?P<Actor2Geo_Long>.*?)\t(?P<Actor2Geo_FeatureID>.*?)\t(?P<ActionGeo_Type>.*?)\t(?P<ActionGeo_FullName>.*?)\t(?P<ActionGeo_CountryCode>.*?)\t(?P<ActionGeo_ADM1Code>.*?)\t(?P<ActionGeo_Lat>.*?)\t(?P<ActionGeo_Long>.*?)\t(?P<ActionGeo_FeatureID>.*?)\t(?P<DateAdded>.*?)\t(?P<SourceUrl>.*?)");



def setupTopic_routerMessage(server,schema_registry_url):
  global regexp_routerMessage
  global avroProducer_routerMessage
  global count_routerMessage
  count_routerMessage=0

  # Topic routerMessage scans for a regex that captures these two line patterns:
  #<31>Apr  1 06:51:38 KKM-WiFi24K-CCR10 8C:00:6D:5D:5E:15 (10.144.49.82): RADIUS accounting request sent
  #<30>Apr  1 06:55:28 KKM-WiFi24K-CCR10 jwe791            (10.211.49.97): trying to log in by http-pap

  avro_schema="""
  {"namespace": "weblog.kkr.avro",
  "type": "record",
  "name": "routerMessage",
  "fields": [
     {"name": "month", "type": ["null","string"],"default":null}
    ,{"name": "day", "type": ["null","string"],"default":null}
    ,{"name": "time", "type": ["null","string"],"default":null}
    ,{"name": "kkm", "type": ["null","string"],"default":null}
    ,{"name": "mac" , "type": ["null","string"],"default":null}
    ,{"name": "ip"  , "type": ["null","string"],"default":null}
    ,{"name": "message"  , "type": ["null","string"],"default":null}
  ]
  }"""
  schema=avro.loads(avro_schema)
  avroProducer_routerMessage = AvroProducer({ 'bootstrap.servers': server, 'schema.registry.url': schema_registry_url}, default_value_schema=schema)

  #'(?P<mac>.{2}:.{2}:.{2}:.{2}:.{2}:.{2})'
  regexp_routerMessage = re.compile(
  ('(<27>|<28>|<30>|<31>)'
  '(?P<month>\D{3})'
  '(?P<whitespace1>\s{1,2})'
  '(?P<day>\d{1,2})'
  '(?P<whitespace2>\s{1,2})'
  '(?P<time>\d{2}:\d{2}:\d{2})'
  '(?P<whitespace3>\s{1,2})'
  '(?P<kkm>\D{3}-.{7}-\D{3}\d{1,2})'
  '(?P<whitespace4>\s{1})'
  '(?P<mac>[^\s]+)'
  '(?P<whitespace5>\s{1})'
  '(?P<open_parentheses>\()'
  '(?P<ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})'
  '(?P<close_parentheses>\))'
  '(?P<colon>\:)'
  '(?P<whitespace6>\s{1})'
  '(?P<message>.*)'
   '.*')
  , re.IGNORECASE)







def setupTopic_routerHttp(server,schema_registry_url):
  global regexp_routerHttp
  global avroProducer_routerHttp
  global count_routerHttp
  count_routerHttp=0

  # Topic routerHttp scans for a regex that returns this pattern:
  # <30>Apr  1 06:51:37 KKM-WiFi24K-CCR10 10.243.56.123 GET http://time.samsungcloudsolution.com/openapi/timesync?client=TimeAgent/1.0  action=allow cache=MISS

  avro_schema="""
  {"namespace": "weblog.kkr.avro",
  "type": "record",
  "name": "routerHttp",
  "fields": [
     {"name": "month"      , "type": ["null","string"],"default":null}
    ,{"name": "day"        , "type": ["null","string"],"default":null}
    ,{"name": "time"       , "type": ["null","string"],"default":null}
    ,{"name": "kkm"        , "type": ["null","string"],"default":null}
    ,{"name": "ip"         , "type": ["null","string"],"default":null}
    ,{"name": "url"        , "type": ["null","string"],"default":null}
    ,{"name": "http_cmd"   , "type": ["null","string"],"default":null}
    ,{"name": "http_action", "type": ["null","string"],"default":null}
    ,{"name": "http_cache" , "type": ["null","string"],"default":null}
  ]
  }"""
  schema=avro.loads(avro_schema)
  avroProducer_routerHttp = AvroProducer({ 'bootstrap.servers': server, 'schema.registry.url': schema_registry_url}, default_value_schema=schema)

  regexp_routerHttp = re.compile(
  ('(<27>|<28>|<30>|<31>)'
  '(?P<month>\D{3})'
  '(?P<whitespace1>\s{1,2})'
  '(?P<day>\d{1,2})'
  '(?P<whitespace2>\s{1,2})'
  '(?P<time>\d{2}:\d{2}:\d{2})'
  '(?P<whitespace3>\s{1,2})'
  '(?P<kkm>\D{3}-.{7}-\D{3}\d{1,2})'
  '(?P<whitespace4>\s{1})'
  '(?P<ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})'
  '(?P<whitespace5>\s{1})'
  '(?P<http_cmd>[^\s]+)'
  '(?P<whitespace6>\s{1,3})'
  '(?P<url>[^\s]+)'
  '(?P<whitespace7>\s{1,3})'
  '(?P<action_string>action=)'
  '(?P<http_action>[^\s]+)'
  '(?P<whitespace8>\s{1,3})'
  '(?P<cache_string>cache=)'
  '(?P<http_cache>[^\s]+)'
   '.*')
  , re.IGNORECASE)





def setupTopic_routerHsBridge(server,schema_registry_url):
  global regexp_routerHSBridge
  global avroProducer_routerHSBridge
  global count_routerHSBridge
  count_routerHSBridge=0

  # Topic routerHSBridge scans for a regexathat returns this pattern:
  #<31>Apr  1 06:51:39 KKM-WiFi24K-CCR10 hs-bridge_CCO_243: new host detected 54:9F:13:6F:3C:3A/10.243.52.180 by TCP :50872 -> 203.113.34.26:80
  avro_schema="""
  {"namespace": "weblog.kkr.avro",
  "type": "record",
  "name": "routerHSBridge",
  "fields": [
     {"name": "month", "type": ["null","string"],"default":null}
    ,{"name": "day", "type": ["null","string"],"default":null}
    ,{"name": "time", "type": ["null","string"],"default":null}
    ,{"name": "kkm", "type": ["null","string"],"default":null}
    ,{"name": "hs_bridge"  , "type": ["null","string"],"default":null}
    ,{"name": "message"  , "type": ["null","string"],"default":null}
  ]
  }"""
  schema=avro.loads(avro_schema)
  avroProducer_routerHSBridge = AvroProducer({ 'bootstrap.servers': server, 'schema.registry.url': schema_registry_url}, default_value_schema=schema)

  regexp_routerHSBridge = re.compile(
  ('(<27>|<28>|<30>|<31>)'
  '(?P<month>\D{3})'
  '(?P<whitespace1>\s{1,2})'
  '(?P<day>\d{1,2})'
  '(?P<whitespace2>\s{1,2})'
  '(?P<time>\d{2}:\d{2}:\d{2})'
  '(?P<whitespace3>\s{1,2})'
  '(?P<kkm>\D{3}-.{7}-\D{3}\d{1,2})'
  '(?P<whitespace4>\s{1})'
  '(?P<hs_bridge>[^\s]+:)'
  '(?P<whitespace5>\s{1})'
  '(?P<message>[^\s]+)'
   '.*')
  , re.IGNORECASE)



def setupTopic_routerElse(server,schema_registry_url):
  global regexp_routerElse
  global avroProducer_routerElse
  global count_routerElse
  count_routerElse=0

  # Topic routerElse scans for a regex that returns this pattern:
  #<31>Apr  1 07:00:06 CWT-WiFi24K-CCR05 .*
  # for example :
  #<31>Apr  1 07:00:06 CWT-WiFi24K-CCR05 already 15 logins in progress\n'
  avro_schema="""
  {"namespace": "weblog.kkr.avro",
  "type": "record",
  "name": "routerElse",
  "fields": [
     {"name": "month", "type": ["null","string"],"default":null}
    ,{"name": "day", "type": ["null","string"],"default":null}
    ,{"name": "time", "type": ["null","string"],"default":null}
    ,{"name": "kkm", "type": ["null","string"],"default":null}
    ,{"name": "message"  , "type": ["null","string"],"default":null}
  ]
  }"""
  schema=avro.loads(avro_schema)
  avroProducer_routerElse = AvroProducer({ 'bootstrap.servers': server, 'schema.registry.url': schema_registry_url}, default_value_schema=schema)

  regexp_routerElse = re.compile(
  ('(<30>|<31>)'
  '(?P<month>\D{3})'
  '(?P<whitespace1>\s{1,2})'
  '(?P<day>\d{1,2})'
  '(?P<whitespace2>\s{1,2})'
  '(?P<time>\d{2}:\d{2}:\d{2})'
  '(?P<whitespace3>\s{1,2})'
  '(?P<kkm>\D{3}-.{7}-\D{3}\d{1,2})'
  '(?P<whitespace4>\s{1,3})'
  '(?P<message>[^\s]+)'
   '.*')
  , re.IGNORECASE)





def load(datafile, schema, server):
    global count_routerMessage
    global count_routerHttp
    global count_routerHSBridge
    global count_routerElse
    global count_gdeltEvent
    #value_schema = avro.load(schema)
    # key_schema = avro.load('KeySchema.avsc')

    #avroProducer = AvroProducer({
    #    'bootstrap.servers': server,
    #    'schema.registry.url': schema_registry_url},
    #    default_value_schema=value_schema)

    #with codecs.open(datafile, 'r', encoding='utf-8', errors='backslashreplace') as csvfile:
    #     spamreader = csv.reader((l.replace('\0', '') for l in csvfile))
    #    for row in spamreader:
    with open(datafile) as f:
      for row in f.readlines():
          #
          # Topic 1: routerMessage
          #
          if re.match(regexp_routerMessage,str(row)):
             match=re.match(regexp_routerMessage,row)
             count_routerMessage+=1
             #print("Match 1! ",row)
             data = {
               'month':match.group('month')
              ,'day':match.group('day')
              ,'time':match.group('time')
              ,'kkm':match.group('kkm')
              ,'mac':match.group('mac')
              ,'ip':match.group('ip')
              ,'time':match.group('time')
              ,'message':match.group('message')
             }
             Produce_routerMessage("routerMessage",data)
             #
          elif re.match(regexp_routerHttp,str(row)):
          #
          # Topic 2: routerHttp
          #
             match=re.match(regexp_routerHttp,row)
             count_routerHttp+=1
             #print("Match 2! #",match.group('url'),"# ",match.group('http_action')," ",match.group('http_cache'))
             data = {
               'month':match.group('month')
              ,'day':match.group('day')
              ,'time':match.group('time')
              ,'kkm':match.group('kkm')
              ,'ip':match.group('ip')
              ,'url':match.group('url')
              ,'http_cmd':match.group('http_cmd')
              ,'http_action':match.group('http_action')
              ,'http_cache':match.group('http_cache')
             }
             Produce_routerHttp("routerHttp",data)
          elif re.match(regexp_routerHSBridge,str(row)):
          #
          # Topic 3: routerHSBridge
          #
             match=re.match(regexp_routerHSBridge,row)
             count_routerHSBridge+=1
             #print("Match 3! ",row)
             data = {
               'month':match.group('month')
              ,'day':match.group('day')
              ,'time':match.group('time')
              ,'kkm':match.group('kkm')
              ,'hs_bridge':match.group('hs_bridge')
              ,'message':match.group('message')
             }
             Produce_routerHSBridge("routerHSBridge",data)
          elif re.match(regexp_routerElse,str(row)):
          #
          # Topic 3: routerElse
          #
             match=re.match(regexp_routerElse,row)
             count_routerElse+=1
             #print("Match 3! ",row)
             data = {
               'month':match.group('month')
              ,'day':match.group('day')
              ,'time':match.group('time')
              ,'kkm':match.group('kkm')
              ,'message':match.group('message')
             }
             Produce_routerElse("routerElse",data)
          elif re.match(regexp_gdeltEvent,str(row)):
          #
          # Topic 4: gdeltEvent
          #
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
        }
             Produce_gdeltEvent("gdeltEvent7",data,'{"gdeltEvent7_key":"'+str(match.group('EventId')+'"}'))
             avroProducer_gdeltEvent.flush()
          else:
           print("NotMatch : ",row)
    print("gdeltEvent",count_gdeltEvent,"routerMessage ",count_routerMessage," routerHttp:",count_routerHttp," routerHSBridge:",count_routerHSBridge," routerElse:",count_routerElse)


def main():
    datafile = ''
    schema = ''
    server = hostname+':9092'

    if len(sys.argv) == 1:
        print("Usage: \n")
        print("python dataload.py datafile schema [server_address ]")
        print("e.g. python dataload_avro.py test_data.csv "
              "schema.avsc localhost:1234 ")
        return

    if len(sys.argv) > 1:
        datafile = sys.argv[1]

    if len(sys.argv) > 2:
        schema = sys.argv[2]

    if len(sys.argv) > 3:
        server = sys.argv[3]


    load(datafile, schema, server)


if __name__ == '__main__':
    server = hostname+':9092'
    setupTopic_routerMessage(server,schema_registry_url)
    setupTopic_routerHttp(server,schema_registry_url)
    setupTopic_routerHsBridge(server,schema_registry_url)
    setupTopic_routerElse(server,schema_registry_url)
    setupTopic_gdeltEvent(server, schema_registry_url)
    main()
    avroProducer_routerMessage.flush()
    avroProducer_routerHttp.flush()
    avroProducer_routerHSBridge.flush()
    avroProducer_routerElse.flush()
    avroProducer_gdeltEvent.flush()