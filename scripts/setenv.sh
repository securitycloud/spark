#!/bin/bash

#first is considered to be the master node
ALL_SERVERS[1]=10.16.31.211
ALL_SERVERS[2]=10.16.31.212
ALL_SERVERS[3]=10.16.31.213
ALL_SERVERS[4]=10.16.31.214
ALL_SERVERS[5]=10.16.31.215

MASTERURL=spark://sc-211:7077

WRK=/root/spark

URL_SPARK=http://mirror.hosting90.cz/apache/spark/spark-1.3.1/spark-1.3.1-bin-hadoop2.6.tgz 
