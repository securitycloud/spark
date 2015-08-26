#!/bin/bash

. bin/setenv.sh

KAFKA_JAR=../kafka/kafka-filip/target/kafka-filip-1.0-SNAPSHOT-jar-with-dependencies.jar


# DOWNLOAD RESULTS
java -cp $KAFKA_JAR cz.muni.fi.kafka.consumer.KafkaConsumer
