#!/bin/bash

# master node
ALL_SERVERS[1]=10.16.31.211
# slave nodes (max servers -1)
NUMBER_OF_SLAVES=2
ALL_SERVERS[2]=10.16.31.212
ALL_SERVERS[3]=10.16.31.213
ALL_SERVERS[4]=10.16.31.214
ALL_SERVERS[5]=10.16.31.215

# kafka
KAFKA_INSTALL=/root/kafka/kafka_2.11-0.8.2.1
KAFKA_PRODUCER=10.16.31.200
KAFKA_CONSUMER=10.16.31.201
SRV_ZK=${ALL_SERVERS[1]}
TESTING_TOPIC=sparkOutput # should match kafka.producer.topic in pom.xml
SERVICE_TOPIC=sparkResults

# url given by the start of spark-master.sh at the master node 
MASTERURL=spark://sc-211:7077

# existing work directory folder on all machines
WRK=/root/spark

# mirror for spark built for haddop tgz
URL_SPARK=http://mirror.hosting90.cz/apache/spark/spark-1.5.0/spark-1.5.0-bin-hadoop2.6.tgz

# console color switches
ERR="\033[1;31m"
OK="\033[1;32m"
LOG="\033[1;34m"
OFF="\033[0m"