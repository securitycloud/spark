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
KAFKA_PRODUCER=10.16.31.200
KAFKA_CONSUMER=10.16.31.201
# url given by the start of spark-master.sh at the master node 
MASTERURL=spark://sc-211:7077
# existing folder on all machines
WRK=/root/spark
# mirror for spark built for haddop tgz
URL_SPARK=http://mirror.hosting90.cz/apache/spark/spark-1.4.0/spark-1.4.0-bin-hadoop2.6.tgz