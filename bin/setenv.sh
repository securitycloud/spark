#!/bin/bash

# master node
ALL_SERVERS[1]=100.64.25.101
# slave nodes (max servers -1)
NUMBER_OF_SLAVES=4
ALL_SERVERS[2]=100.64.25.102
ALL_SERVERS[3]=100.64.25.103
ALL_SERVERS[4]=100.64.25.104
ALL_SERVERS[5]=100.64.25.105

# kafka
KAFKA_INSTALL=/home/securitycloud/kafka/kafka_2.9.2-0.8.2.1
KAFKA_PRODUCER=100.64.25.107
KAFKA_CONSUMER=100.64.25.107
SRV_ZK=${ALL_SERVERS[1]}
TESTING_TOPIC=sparkOut
SERVICE_TOPIC=sparkResults

# url given by the start of spark-master.sh at the master node 
MASTERURL=spark://sc1:7077

# existing folder on all machines
WRK=/home/securitycloud/spark

# mirror for spark built for haddop tgz
URL_SPARK=http://d3kbcqa49mib13.cloudfront.net/spark-1.4.1-bin-hadoop2.6.tgz

# console color switches
ERR="\033[1;31m"
OK="\033[1;32m"
LOG="\033[1;34m"
OFF="\033[0m"
