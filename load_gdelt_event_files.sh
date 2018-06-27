#!/bin/ksh
#
# Auto load a bunch of gdelt EVENT files into kafka
#
for i in `find ~/data/GDELT/events/* -print`
do
  echo processing $i
  python AvroProducer.py ${i}
  echo "Paused..."
  read hello
done

