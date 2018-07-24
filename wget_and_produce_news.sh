#!/bin/bash
#
P=`pwd`
HOST=`hostname`
if [ "$HOST" = "Mark-Teehan-MBP13.local" -o "$HOST" = "MarkTeehanMBP13"  ]
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
  ${CONFLUENT_HOME}/bin/kafka-topics --create --zookeeper ${KAFKA_HOST}:2181 --topic $TOPIC --partitions 25 --replication-factor 1
}

load_kafka_topic()
{
  TOPIC=$1
  FILE=$2
  HOST=$3
  cd $PYDIR
  /usr/bin/python GDELT_AvroProducer.py ${TOPIC} ${F} ${HOST} > /tmp/${TOPIC}_load.txt
  export ROWS_PRODUCED=`cat /tmp/${TOPIC}_load.txt|grep "Loaded topic"|awk -F: '{print $2}'`
  export ROWS_PRODUCED_MESSAGE="(I) Rows produced to topic ${TOPIC} is $ROWS_PRODUCED"
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
   echo "${ROWS_PRODUCED_MESSAGE},  offset check for topic $TOPIC is $ROWS"
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
 RETRY=$2
 
 wget --quiet $FILE
 if [ -f `basename $FILE` ]
 then
  return -1
 else
  if [ "$RETRY" = "RETRY" ]
  then
    echo "(W) $FILE not yet available, sleep 30 and retry"
    sleep 30
    wget $FILE
    if [ -f `basename $FILE` ]
    then
     echo "(I) success. Continuing"
     return 0
    fi
  fi
fi
}

function get_last_utc_interval()
{
NOW_MIN=`date +"%M"`
for i in {00..14}
do
  LPAD_I=`printf "%02d\n" ${i}`
if [ "$NOW_MIN" = "$LPAD_I" ]
  then
    MIN="00"
  fi
done
for i in {15..29}
do
  LPAD_I=`printf "%02d\n" ${i}`
  if [ "$NOW_MIN" = "$LPAD_I" ]
  then
    MIN="15"
  fi
done
for i in {30..44}
do
  LPAD_I=`printf "%02d\n" ${i}`
  if [ "$NOW_MIN" = "$LPAD_I" ]
  then
    MIN="30"
  fi
done
for i in {45..59}
do
  LPAD_I=`printf "%02d\n" ${i}`
  if [ "$NOW_MIN" = "$LPAD_I" ]
  then
    MIN="45"
  fi
done
echo `date -u +"%Y%m%d%H"`${MIN}00
exit 
}


get_latest_file()
{
DT=$(get_last_utc_interval)
echo "Time is ${DT} - fetching the next GDELT file!"
cd $D
rm -f ${DT}*
wgetfile http://data.gdeltproject.org/gdeltv2/${DT}.export.CSV.zip DONT_RETRY

if [ -f `basename ${DT}.export.CSV.zip` ]
  then
    rm -f ${DT}.export.CSV
    unzip -D -qq ${DT}.export.CSV.zip
    rm ${DT}.export.CSV.zip
    export F=${D}/${DT}.export.csv
    mv ${DT}.export.CSV ${F}
    MON=`date +"%Y%m"`
    create_kafka_topic GDELT_EVENT02
    load_kafka_topic GDELT_EVENT02 ${F} $KAFKA_HOST
    TARGET_ROWS=`cat $F | wc -l|sed 's/ //g'`
    check_topic_count GDELT_EVENT02 ${F} $TARGET_ROWS
fi
}  # end of get_latest_file



get_file()
{
  #DT2=`date -u +"%Y%m%d%H"`${MIN}
  DT2=$1
  RETRY=$2
  CURRENT_UTC_INTERVAL=$3
  DT=${DT2}00
  if [ $DT = $CURRENT_UTC_INTERVAL ]
  then
    PREFIX="CURRENT>"
  else
    PREFIX="        "
  fi
  if [ $DT -gt $CURRENT_UTC_INTERVAL ]
  then
   # dont search for future files
   return -1
  fi
  cd $D
  if [ -f ${DT}*  ]
  then
   FILE=`ls -l1 ${DT}*`
   echo "(I) ${PREFIX} Data for ${DAY} ${HOUR} ${INTERVAL} already loaded. Skipping ${FILE}"
   return 1
  else
    wgetfile http://data.gdeltproject.org/gdeltv2/${DT}.export.CSV.zip DONT_RETRY
    if [ -f "${DT}.export.CSV.zip" ]
    then
        echo "(I) ${PREFIX} Data for ${DAY} ${HOUR} ${INTERVAL} - downloading ${FILE} from the GDELT server"
        FILE=`ls -1 ${DT}.export.CSV.zip`
        rm -f ${DT}.export.CSV
        unzip -qq ${DT}.export.CSV.zip
        rm -f ${DT}.export.CSV.zip
        export F=${D}/${DT}.export.csv
        mv ${DT}.export.CSV ${F}
    else
        echo "(I) ${PREFIX} Data for ${DAY} ${HOUR} ${INTERVAL} - unable to find file on GDELT server ${FILE}"
    fi
  fi
}  # end of get_file


load_file()
{
  #DT2=`date -u +"%Y%m%d%H"`${MIN}
  DT2=$1
  RETRY=$2
  CURRENT_UTC_INTERVAL=$3
  DT=${DT2}00
  if [ $DT = $CURRENT_UTC_INTERVAL ]
  then
    PREFIX="CURRENT>"
  else
    PREFIX="        "
  fi
  if [ $DT -gt $CURRENT_UTC_INTERVAL ]
  then
   # dont search for future files
   return -1
  fi
  cd $D
  export F=${D}/${DT}.export.csv

  if [ -f "${F}" ]
    then
        MON=`date +"%Y%m"`
        load_kafka_topic GDELT_EVENT02 ${F} $KAFKA_HOST
        TARGET_ROWS=`cat $F | wc -l|sed 's/ //g'`
        check_topic_count GDELT_EVENT02 ${F} $TARGET_ROWS
    else
        echo "(I) ${PREFIX} Data for ${DAY} ${HOUR} ${INTERVAL} - unable to find file ${FILE}"
    fi
}  # end of get_file

days=$1
if [ "$days"x == x ]
then
  echo "(E) usage downloads_gdelt_days.sh days [reload]"
  echo "    where days is an integer of the nunmber of prior days to reload"
  exit 255
fi

DT2=`date -u +"%Y%m%d"`
start_date=$(date -v -${days}d +"%Y%m%d")
INTERVALS="00 15 30 45"

CURRENT_UTC_INTERVAL=$(get_last_utc_interval)
echo "Starting with timestamp $start_date"
create_kafka_topic GDELT_EVENT02
for ((i=$days;i>=0;i--))
do
  if [ $i -gt 0 ]
  then
    DAY=$(date -v -${i}d +"%Y%m%d")
  else
    DAY=$(date +"%Y%m%d")
  fi
  for HOUR_1 in {00..23}
  do
    HOUR=`printf "%02d\n" ${HOUR_1}`
    for INTERVAL in `echo ${INTERVALS}`
    do
      #get_file ${DAY}${HOUR}${INTERVAL} DONT_RETRY $CURRENT_UTC_INTERVAL
      load_file ${DAY}${HOUR}${INTERVAL} DONT_RETRY $CURRENT_UTC_INTERVAL
    done
  done
done
    

