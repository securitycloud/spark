#!/bin/bash

. bin/setenv.sh

BATCH_SIZE[1]=5000

TESTTYPES[1]=ReadWriteTest
TESTTYPES[2]=FilterIPTest
TESTTYPES[3]=CountTest
TESTTYPES[4]=AggregationTest
TESTTYPES[5]=TopNTest
TESTTYPES[6]=SynScanTest

COMPUTERS[1]=1
COMPUTERS[2]=3
COMPUTERS[3]=5

REPEAT=5

NUM_TESTS=${#TESTTYPES[@]}
NUM_TESTS=$((NUM_TESTS * ${#BATCH_SIZE[@]}))
NUM_TESTS=$((NUM_TESTS * ${#COMPUTERS[@]}))
NUM_TESTS=$((NUM_TESTS * ${REPEAT}))
ACT_TEST=1

bin/kill-cluster.sh
#bin/clean-cluster.sh
#bin/install-cluster.sh
bin/start-cluster.sh

echo -e $LOG Recreating input topic $SERVICE_TOPIC on $KAFKA_CONSUMER $OFF
bin/run-topic.sh $SERVICE_TOPIC 1 $KAFKA_CONSUMER

for BS in "${BATCH_SIZE[@]}"
do
    for PC in "${COMPUTERS[@]}"
    do
        echo -e $LOG Recreating input topic $TESTING_TOPIC with $PC partitions on $KAFKA_PRODUCER $OFF
        bin/run-topic.sh $TESTING_TOPIC $PC $KAFKA_PRODUCER

        bin/run-input.sh $BS

        for i in `seq 1 $REPEAT`
        do
            for TEST in "${TESTTYPES[@]}"
            do
                echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
                bin/run-test-read.sh $TEST $PC $PC $BS
                ACT_TEST=$((ACT_TEST + 1))
            done
        done
    done
done

# bin/result-download.sh | bin/result-parse.sh > out

#bin/kill-cluster.sh
