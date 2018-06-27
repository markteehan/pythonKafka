#!/bin/sh

#
# iterate over [ YEAR ] [ MONTH ] DAY downloading a .csv.zip of global news stories per day
#

DIR=/Users/markteehan/Data/GDELT/events
mkdir -p ${DIR} 2>/dev/null
cd ${DIR}

TS=`date +"%Y%m%d"`

YEAR="2018"
MONTHS="06"
DAYS="01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31"
for i1 in `echo $MONTHS`
do
  for i2 in `echo $DAYS`
  do
         DAY=${YEAR}${i1}${i2}
        FILE=${DAY}.export.CSV
        echo "(I) processing ${FILE}"
        EXISTS=`cat ${TS}_hdfs_files.txt | grep ${DAY} | wc -l`
        if [ "$EXISTS" -eq 1 ]
        then
         echo "(I) ${DAY} is on HDFS. Continuing"
        fi
        if [ "$EXISTS" -eq 0 ]
        then
          if [ "${DAY}" -lt "${TS}" ]
          then
             echo "(I) ${DAY} can be downloaded."
             wget http://data.gdeltproject.org/events/${FILE}.zip
             RET=$?
             if [ "$RET" -eq 0 ]
             then
               echo "(I) ${DAY} downloaded. Pushing to HDFS"
               unzip ${FILE}.zip
               #hadoop fs -put -f ${FILE} data/gdelt-events-raw
               #hadoop fs -put -f ${FILE} data/gdelt-events-raw-oneday
               #merge_one_day()
               #hadoop fs -rm data/gdelt-events-raw-oneday/${FILE}
               ##hadoop fs -ls data/gdelt-events-raw/${FILE}
               rm ${FILE}.zip
             else
               echo "(E) Investigate error for wget http://data.gdeltproject.org/events/${FILE}.zip"
             fi
           fi
        fi
        sleep 2
      done
   done
mv ${TS}_hdfs_files.txt old