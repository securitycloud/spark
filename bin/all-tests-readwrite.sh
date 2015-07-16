#!/bin/bash

. bin/setenv.sh

FILTERS[1]=
FILTERS[2]=62.148.241.49

BATCH_SIZE[1]=5000
BATCH_SIZE[2]=1000

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

bin/clean-cluster.sh
bin/install-cluster.sh
bin/start-cluster.sh
sleep 20

echo -e $LOG Recreating input topic $SERVICE_TOPIC on $KAFKA_CONSUMER $OFF
bin/run-topic.sh $SERVICE_TOPIC 1 $KAFKA_CONSUMER

for FILTER in "${FILTERS[@]}"
do
    for BS in "${BATCH_SIZE[@]}"
    do
        for PTN in "${PARTITIONS[@]}"
        do
            for PC in "${COMPUTERS[@]}"
            do
                echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
                bin/run-test-readwrite.sh $FILTER $PC $PTN $BS
                ACT_TEST=$((ACT_TEST + 1))
            done
        done
    done
done

# bin/result-download.sh | bin/result-parse.sh > out
