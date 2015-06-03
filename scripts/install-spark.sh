#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    echo "You must specify server"
    exit 1;
fi

SERVER=$1

# DOWNLOAD AND EXTRACT
ssh root@$SERVER "
    cd $WRK
    wget -q $URL_SPARK -O spark.tgz
    mkdir spark-bin-hadoop
    tar -xzvf spark.tgz -C spark-bin-hadoop --strip 1
    rm spark.tgz
"
