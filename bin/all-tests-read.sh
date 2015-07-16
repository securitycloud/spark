#!/bin/bash

. bin/setenv.sh

FILTERS[1]=
FILTERS[2]=62.148.241.49

BATCH_SIZE[1]=5000
#BATCH_SIZE[2]=1000

PARTITIONS[1]=1
PARTITIONS[2]=3
PARTITIONS[3]=5

COMPUTERS[1]=1
COMPUTERS[2]=2
COMPUTERS[3]=3
COMPUTERS[4]=4
COMPUTERS[5]=5

NUM_TESTS=${#FILTERS[@]}
NUM_TESTS=$((NUM_TESTS * ${#BATCH_SIZE[@]}))
NUM_TESTS=$((NUM_TESTS * ${#PARTITIONS[@]}))
NUM_TESTS=$((NUM_TESTS * ${#COMPUTERS[@]}))
ACT_TEST=1

bin/kill-cluster.sh
#bin/clean-cluster.sh
#bin/install-cluster.sh
bin/start-cluster.sh

echo -e $LOG Recreating input topic $SERVICE_TOPIC on $KAFKA_CONSUMER $OFF
bin/run-topic.sh $SERVICE_TOPIC 1 $KAFKA_CONSUMER

for BS in "${BATCH_SIZE[@]}"
do
    for PTN in "${PARTITIONS[@]}"
    do
        echo -e $LOG Recreating input topic $TESTING_TOPIC with $PTN partitions on $KAFKA_PRODUCER $OFF
        bin/run-topic.sh $TESTING_TOPIC $PTN $KAFKA_PRODUCER

        bin/run-input.sh $BS

        for PC in "${COMPUTERS[@]}"
        do
            for FILTER in "${FILTERS[@]}"
            do
                echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
                bin/run-test-read.sh $PC $PTN $BS $FILTER
                ACT_TEST=$((ACT_TEST + 1))
            done
        done
    done
done

# bin/result-download.sh | bin/result-parse.sh > out

#bin/kill-cluster.sh
