#!/bin/bash

. bin/setenv.sh

BATCH_SIZE[1]=5000

#TESTTYPES[1]=ReadWriteTest
#TESTTYPES[2]=FilterIPTest
TESTTYPES[1]=CountTest
TESTTYPES[2]=AggregationTest
TESTTYPES[3]=TopNTest
TESTTYPES[4]=SynScanTest

COMPUTERS[1]=5
#COMPUTERS[2]=3
#COMPUTERS[3]=1

REPEAT=2

NUM_TESTS=${#TESTTYPES[@]}
NUM_TESTS=$((NUM_TESTS * ${#BATCH_SIZE[@]}))
NUM_TESTS=$((NUM_TESTS * ${#COMPUTERS[@]}))
NUM_TESTS=$((NUM_TESTS * ${REPEAT}))
ACT_TEST=1


echo -e $LOG Recreating output performance result topic$ERR $SERVICE_TOPIC$LOG on $KAFKA_CONSUMER $OFF
bin/run-topic.sh $SERVICE_TOPIC 1 $KAFKA_CONSUMER > /dev/null
echo -e $LOG Recreating output test result topic$ERR $TESTING_TOPIC$LOG on $KAFKA_CONSUMER $OFF
bin/run-topic.sh $TESTING_TOPIC 1 $KAFKA_CONSUMER > /dev/null

for BS in "${BATCH_SIZE[@]}"
do
    for PC in "${COMPUTERS[@]}"
    do
        #echo -e $LOG Recreating input topic $TESTING_TOPIC with $PC partitions on $KAFKA_PRODUCER $OFF
        #bin/run-topic.sh $TESTING_TOPIC $PC $KAFKA_PRODUCER

        #bin/run-input.sh $BS
        SLAVES_COUNT=$((PC - 1))
        sed -i "6s/.*/NUMBER_OF_SLAVES=${SLAVES_COUNT}/" bin/setenv.sh
        bin/restart-cluster.sh #> /dev/null 2>&1
        echo -e $OK restarted cluster for ${SLAVES_COUNT} slaves $OFF

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

bin/kill-cluster.sh