#!/bin/bash

# master node
ALL_SERVERS[1]=localhost
# slave nodes (max servers -1)
NUMBER_OF_SLAVES=0

# kafka
KAFKA_INSTALL=/home/filip/kafka_2.11-0.8.2.2
#KAFKA_INSTALL=/home/filip/kafka_2.11-0.10.1.1
KAFKA_PRODUCER=localhost
KAFKA_CONSUMER=localhost

TESTING_TOPIC=sparkOutput # should match kafka.producer.topic in pom.xml
SERVICE_TOPIC=sparkResults

# existing work directory folder on all machines
WRK=/home/filip/spark-work-dir

# mirror for spark built for hadoop tgz
URL_SPARK=http://d3kbcqa49mib13.cloudfront.net/spark-1.6.3-bin-hadoop2.6.tgz

# console color switches
ERR="\033[1;31m"
OK="\033[1;32m"
LOG="\033[1;34m"
OFF="\033[0m"

#TESTTYPES[1]=ReadWriteTest
#TESTTYPES[2]=FilterIPTest
TESTTYPES[3]=CountTest
TESTTYPES[4]=AggregationTest
TESTTYPES[5]=TopNTest
TESTTYPES[6]=SynScanTest
TESTTYPES[7]=Statistics

REPEAT=1

# compute total test count
NUM_TESTS=${#TESTTYPES[@]}
NUM_TESTS=$((NUM_TESTS * ${REPEAT}))
# current test
ACT_TEST=1

# location of your spark folder
#export SPARK_HOME="/home/filip/spark-1.6.3-bin-hadoop2.6"
export SPARK_HOME="/home/filip/spark-2.1.0-bin-hadoop2.7"

#$KAFKA_INSTALL/bin/zookeeper-server-start.sh config/zookeeper.properties #--- DO THIS MANUALLY, start Zookeeper
#add delete.topic.enable=true to $KAFKA_INSTALL/config/server.properties
#$KAFKA_INSTALL/bin/kafka-server-start.sh config/server.properties #--- DO THIS MANUALLY, start Kafka

echo -e $LOG Recreating output performance result topic$ERR $SERVICE_TOPIC$LOG on $KAFKA_CONSUMER $OFF
$KAFKA_INSTALL/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $SERVICE_TOPIC > /dev/null
$KAFKA_INSTALL/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $SERVICE_TOPIC > /dev/null
echo -e $LOG Recreating output test result topic$ERR $TESTING_TOPIC$LOG on $KAFKA_CONSUMER $OFF
$KAFKA_INSTALL/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $TESTING_TOPIC > /dev/null
$KAFKA_INSTALL/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $TESTING_TOPIC > /dev/null


if true;
    then
        echo  preparing working directory
        # clean
        #kill $(ps aux | grep spark-work-dir | awk '{print $2}')
        #killall java
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
        # install spark
#        cd $WRK
#        echo  installing spark on ${ALL_SERVERS[1]}
#        wget -q --show-progress $URL_SPARK -O spark.tgz
#        mkdir spark-bin-hadoop
#        tar -xzvf spark.tgz -C spark-bin-hadoop --strip 1 > /dev/null
#        rm spark.tgz
        echo -e $OK working dir ready $OFF
fi

for i in `seq 1 $REPEAT`
do
    for TEST in "${TESTTYPES[@]}"
    do
        echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
        # submit app to spark-submit
        cd ${WRK}
        cd project
        killall screen
        screen -S sparktest -d -m mvn exec:exec -Dspark.machines=3 -Dspark.testtype=$TEST -P local
#       mvn exec:exec -Dspark.machines=3 -Dspark.testtype=$TEST -P local
        echo -e ${OK} Test in progress... spark monitor at http://${ALL_SERVERS[1]}:4040
        # wait for test result message, to know the test has finished
        $KAFKA_INSTALL/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ${SERVICE_TOPIC} --max-messages 1
        # restart cluster
        echo -e ${OFF} Restarting environment
#        screen -S sparktest -X quit
        killall screen
        ACT_TEST=$((ACT_TEST + 1))
    done
done