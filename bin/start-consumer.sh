#!/bin/bash
. bin/setenv.sh
# script for starting kafka console consumer at 10.16.31.201
if [ -z "$1" ] 
then
    echo "You must specify topic name"
    exit 1;
fi

ssh $KAFKA_CONSUMER "
	cd kafka/kafka_2.11-0.8.2.1
	bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic $1 --from-beginning
"
