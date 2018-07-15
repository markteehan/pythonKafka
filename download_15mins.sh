#!/bin/ksh
#
P=`pwd`
HOST=`hostname`
if [ "$HOST" = "Mark-Teehan-MBP13.local" ]
then
  KAFKA_HOST="Mark-Teehan-MBP13.local"
  D=/Users/teehan/data/GDELT/events/fifteenmins
  . /Users/teehan/.profile
 mkdir -p $D 2>/dev/null
 PYDIR=~/PycharmProjects/pythonKafka
fi

if [ "$HOST" = "marks-Macbook.local" ]
then
  KAFKA_HOST=marks-MacBook.local
  D=/Users/markteehan/data/GDELT/events/fifteenmins
  . /Users/markteehan/.profile
  mkdir -p $D 2>/dev/null
 PYDIR=~/PycharmProjects/pythonKafka 
fi

MIN=`date -u +"%M"`



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
  HOST=$3
  cd $PYDIR
  /usr/bin/python GDELT_AvroProducer.py ${TOPIC} ${F} ${HOST} > /tmp/${TOPIC}_load.txt
  export ROWS_PRODUCED=`cat /tmp/${TOPIC}_load.txt|grep "Loaded topic"|awk -F: '{print $2}'`
 
  echo "(I) Rows produced to topic ${TOPIC} is $ROWS_PRODUCED"
}


check_topic_count()
{
  TOPIC=$1
  F=$2
  TARGET_ROWS=$3
  ROWS=`$CONFLUENT_HOME/bin/kafka-run-class kafka.tools.GetOffsetShell --broker-list $KAFKA_HOST:9092 --topic $TOPIC --time -1`
  RETURN=$?
  #if [ "$RETURN" -eq "0" ]
  if [ "$ROWS_PRODUCED" -eq "$TARGET_ROWS" ]
  then
   echo "(I) Offset check for topic $TOPIC is $ROWS"
  else
    echo "(E) Offset check for topic $TOPIC returned a return code of $RETURN"
    echo "(I) Bouncing kafka..."
    #bounce_kafka
    #create_kafka_topic $TOPIC
    #load_kafka_topic $TOPIC $F $KAFKA_HOST
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

for i in {0..14}
do
  if [ "$MIN" = "$i" ]
  then
    MIN="00"
    echo "Will pick up the file for $MIN"
  fi
done
for i in {15..29}
do
  if [ "$MIN" = "$i" ]
  then
    MIN="15"
    echo "Will pick up the file for $MIN"
  fi
done
for i in {30..44}
do
  if [ "$MIN" = "$i" ]
  then
    MIN="30"
    echo "Will pick up the file for $MIN"
  fi
done
for i in {45..59}
do
  if [ "$MIN" = "$i" ]
  then
    MIN="45"
    echo "Will pick up the file for $MIN"
  fi
done


  DT2=`date -u +"%Y%m%d%H"`${MIN}
  DT=${DT2}00
  #sleep 45
  echo "Time is ${DT} - fetching the next GDELT file!"
  cd $D
  rm -f ${DT}*
  wgetfile http://data.gdeltproject.org/gdeltv2/${DT}.export.CSV.zip
  echo;echo;echo "This is the downloaded file:"
  ls -l ${DT}.export.CSV.zip
  echo;echo;sleep 2

  if [ -f ${DT}.export.CSV.zip ]
  then
    rm -f ${DT}.export.CSV
    unzip ${DT}.export.CSV.zip
    #rm ${DT}.export.CSV.zip
    export F=${D}/${DT}.export.csv
    mv ${DT}.export.CSV ${F}
    MON=`date +"%Y%m"`
    #create_kafka_topic GDELT_EVENT
    load_kafka_topic GDELT_EVENT ${F} $KAFKA_HOST
    TARGET_ROWS=`cat $F | wc -l|sed 's/ //g'`
    check_topic_count GDELT_EVENT ${F} $TARGET_ROWS
 else
   echo "wget http://data.gdeltproject.org/gdeltv2/${DT}.export.CSV.zip" >  ${DT}.export.CSV.zip.MISSING
 fi
