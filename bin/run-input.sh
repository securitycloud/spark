#!/bin/bash

. bin/setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify Batch size $OFF
    exit 1;
fi
BATCH_SIZE=$1

KAFKA_JAR=$WRK/kafka/kafka-filip/target/kafka-filip-1.0-SNAPSHOT-jar-with-dependencies.jar

# LOG
echo -e $LOG Start producing flows on $KAFKA_PRODUCER $OFF

# PRODUCING FLOWS
ssh $KAFKA_PRODUCER "
    java -jar $KAFKA_JAR $BATCH_SIZE
"
