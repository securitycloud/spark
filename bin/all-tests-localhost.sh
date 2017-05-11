#!/bin/bash
# script for testing on localhost / single machine
# you need to set KAFKA_INSTALL (KAFKA_HOME) to your installations of KAFKA and adjust spark.home property in pom.xml
# add delete.topic.enable=true to $KAFKA_INSTALL/config/server.properties, have zk and kafka running

# kafka home (spark home is set in pom.xml)
KAFKA_INSTALL=/home/filip/kafka_2.11-0.8.2.2

# existing working directory
WRK=/home/filip/spark-work-dir

# input stream topic is only set as kafka.consumer.topic in pom.xml
# topic for test output
TESTING_TOPIC=sparkOutput # should match kafka.producer.topic in pom.xml
# topic for test performance results
SERVICE_TOPIC=sparkResults # should match kafka.producer.resultsTopic in pom.xml

# console color switches
ERR="\033[1;31m"
OK="\033[1;32m"
LOG="\033[1;34m"
OFF="\033[0m"

TESTTYPES[1]=ReadWriteTest
TESTTYPES[2]=FilterIPTest
TESTTYPES[3]=CountTest
TESTTYPES[4]=AggregationTest
TESTTYPES[5]=TopNTest
TESTTYPES[6]=SynScanTest

REPEAT=1

# compute total test count
NUM_TESTS=${#TESTTYPES[@]}
NUM_TESTS=$((NUM_TESTS * ${REPEAT}))
# current test
ACT_TEST=1

echo -e $LOG Recreating output performance result topic$ERR $SERVICE_TOPIC$LOG on localhost $OFF
$KAFKA_INSTALL/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $SERVICE_TOPIC > /dev/null
$KAFKA_INSTALL/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $SERVICE_TOPIC > /dev/null
echo -e $LOG Recreating output test result topic$ERR $TESTING_TOPIC$LOG on localhost $OFF
$KAFKA_INSTALL/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $TESTING_TOPIC > /dev/null
$KAFKA_INSTALL/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $TESTING_TOPIC > /dev/null


# optional, only needed once
if true;
    then
        echo  preparing working directory
        # clean
        rm -rf $WRK/*
        mkdir $WRK > /dev/null 2>&1
        # pack and copy project to work dir
        tar -cf project.tar src pom.xml
        cp project.tar $WRK
        rm project.tar
        # extract project
        cd $WRK
        mkdir project
        tar -xf project.tar -C project
        rm project.tar
        cd project
        mvn clean package -P local > /dev/null
        echo -e $OK working dir ready $OFF
fi

for i in `seq 1 $REPEAT`
do
    for TEST in "${TESTTYPES[@]}"
    do
        echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
        # submit app to spark-submit
        cd ${WRK}/project
        killall screen
        kill `ps aux | grep spark | grep -v grep | awk '{print $2}'`
        screen -S sparktest -d -m mvn exec:exec -Dspark.machines=3 -Dspark.testtype=$TEST -P local
        echo -e ${OK} Test in progress... spark monitor at http://localhost:4040
        # wait for test result message, to know the test has finished
        $KAFKA_INSTALL/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ${SERVICE_TOPIC} --max-messages 1
        # restart cluster
        echo -e ${OFF} Restarting environment
        ACT_TEST=$((ACT_TEST + 1))
    done
done

killall screen
kill `ps aux | grep spark | grep -v grep | awk '{print $2}'`