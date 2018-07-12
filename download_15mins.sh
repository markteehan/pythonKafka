#!/bin/ksh
# 1. download data from GDELT every 15 minutes
# 2. Copy it into sghana16.onprem MapR  HDFS
# 2. Copy it into sghana16.onprem MapR  HDFS for VORA
# 2. Copy it into sghana24.onprem MapR  HDFS
# 2. Copy it into sghana24.onprem MapR  HDFS for VORA
# 2. Copy the file to sghaan21:/tmp and Load the new data to HANA sghana21:PPN.gdelt.EVENT
# 3. Copy the file to altiscale
#
set -x
P=`pwd`
D=/Users/markteehan/data/GDELT/events/fifteenmins
MIN=`date -u +"%M"`
. /Users/markteehan/.profile
KAFKA_HOST=marks-MacBook.local

F=/mapr/pentos.cluster.com/pentos-data/dir_gdelt_EVENT
VIRTUAL_ENV=/Users/markteehan/Dropbox/PycharmProjects/pythonKafka
PATH=/Users/markteehan/Dropbox/PycharmProjects/pythonKafka/bin:/Users/markteehan/confluent-4.1.1/bin:/Users/markteehan/confluent-4.1.1/lib:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/Users/markteehan/confluent-4.1.1/bin:/Users/markteehan/confluent-4.1.1/lib


bounce_kafka()
{
  confluent stop
  confluent start
}
create_kafka_topic()
{
  TOPIC=$1
  ${CONFLUENT_HOME}/bin/kafka-topics --create --zookeeper ${KAFKA_HOST}:2181 --topic $TOPIC --partitions 1 --replication-factor 1
}

load_kafka_topic()
{
  TOPIC=$1
  FILE=$2
  cd /Users/markteehan/PycharmProjects/pythonKafka
  /Users/markteehan/Dropbox/PycharmProjects/pythonKafka/bin/python GDELT_AvroProducer.py ${TOPIC} ${F} > /tmp/${TOPIC}_load.txt
  export ROWS_PRODUCED=`cat /tmp/${TOPIC}_load.txt|grep "Loaded topic"|awk -F: '{print $2}'`
 
  echo "(I) Rows produced to topic ${TOPIC} is $ROWS_PRODUCED"
}


check_topic_count()
{
  TOPIC=$1
  F=$2
  TARGET_ROWS=$3
  set -x
  ROWS=`$CONFLUENT_HOME/bin/kafka-run-class kafka.tools.GetOffsetShell --broker-list $KAFKA_HOST:9092 --topic $TOPIC --time -1`
  RETURN=$?
  #if [ "$RETURN" -eq "0" ]
  if [ "$ROWS_PRODUCED" -eq "$TARGET_ROWS" ]
  then
   echo "(I) Offset check for topic $TOPIC is $ROWS"
   return $ROWS
  else
    echo "(E) Offset check for topic $TOPIC returned a return code of $RETURN"
    echo "(I) Bouncing kafka..."
    #bounce_kafka
    #create_kafka_topic $TOPIC
    #load_kafka_topic $TOPIC $F
    #ROWS2=`$CONFLUENT_HOME/bin/kafka-run-class kafka.tools.GetOffsetShell --broker-list $KAFKA_HOST:9092 --topic $TOPIC --time -1`
    #echo "(I) After bouncing kafka, rowcount for $TOPIC is $ROWS. Finished"
  fi
}

wgetfile()
{
 FILE=$1
 wget $FILE
 if [ -f `basename $FILE` ]
 then
  return 0
 else
  echo "(W) $FILE not yet available, sleep 30 and retry"
  sleep 30
  wget $FILE
  if [ -f `basename $FILE` ]
  then
   echo "(I) success. Continuing"
   return 0
  fi
fi
}

if [ ${MIN} -eq "00" -o ${MIN} -eq "15" -o ${MIN} -eq "30" -o ${MIN} -eq "45" ]
then
  set -x
  DT=`date -u +"%Y%m%d%H%M00"`
  sleep 45
  echo "Time is ${DT} - fetching the next GDELT file!"
  cd /Users/markteehan/data/GDELT/events/fifteenmins
  wgetfile http://data.gdeltproject.org/gdeltv2/${DT}.export.CSV.zip

  if [ -f ${DT}.export.CSV.zip ]
  then
    unzip ${DT}.export.CSV.zip
    rm ${DT}.export.CSV.zip
    export F=${D}/${DT}.export.csv
    mv ${DT}.export.CSV ${F}
    set -x
    MON=`date +"%Y%m"`
    create_kafka_topic GDELT_EVENT_${DT}
    load_kafka_topic GDELT_EVENT_${DT} ${F}
    TARGET_ROWS=`cat $F | wc -l|sed 's/ //g'`
    check_topic_count GDELT_EVENT_${DT} ${F} $TARGET_ROWS
    
 else
   echo "wget http://data.gdeltproject.org/gdeltv2/${DT}.export.CSV.zip" >  ${DT}.export.CSV.zip.MISSING
 fi
fi
