#!/bin/bash

. bin/setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify Number of computers $OFF
    exit 1;
fi
COMPUTERS=$1

if [ -z "$2" ] 
then
    echo -e $ERR You must specify Number of partitions $OFF
    exit 2;
fi
PARTITIONS=$3

if [ -z "$3" ] 
then
    echo -e $ERR You must specify Batch size $OFF
    exit 3;
fi
BATCH_SIZE=$3

if [ -z "$4" ] 
then
    $FILTER=""
    #echo -e $ERR You must specify Filter $OFF
    #exit 4;
else
    FILTER=$4
fi


echo -e $LOG Recreating input topic $TESTING_TOPIC with $PARTITIONS partitions on $KAFKA_PRODUCER $OFF
bin/run-topic.sh $TESTING_TOPIC $PARTITIONS $KAFKA_PRODUCER

echo -e $LOG Recreating output topic $TESTING_TOPIC with 1 partitions on $KAFKA_CONSUMER $OFF
bin/run-topic.sh $TESTING_TOPIC 1 $KAFKA_CONSUMER

STORM_EXE=$WRK/storm/bin/storm
STORM_JAR=$WRK/project/target/storm-1.0-SNAPSHOT-jar-with-dependencies.jar

echo -e $LOG Running on $COMPUTERS computers, filter $FILTER $OFF
ssh root@$SRV_NIMBUS "
    $STORM_EXE jar $STORM_JAR cz.muni.fi.storm.$TOPOLOGY $COMPUTERS $KAFKA_PRODUCER $KAFKA_CONSUMER false
"

echo -e $LOG Logging info to service topic: $SERVICE_TOPIC $OFF
ssh root@$KAFKA_CONSUMER "
    echo Type=readwrite, Filter=$FILTER, Computers=$COMPUTERS, Partitions=$PARTITIONS, BatchSize=$BATCH_SIZE |
        $KAFKA_INSTALL/bin/kafka-console-producer.sh --topic $SERVICE_TOPIC --broker-list localhost:9092
"

bin/run-input.sh $BATCH_SIZE

bin/kill-topology.sh $TOPOLOGY
