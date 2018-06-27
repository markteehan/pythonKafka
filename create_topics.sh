cd $CONFLUENT_HOME

bin/kafka-topics --create --zookeeper localhost:2181 --topic gdeltEvent6 --partitions 8 --replication-factor 1
