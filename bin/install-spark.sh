#!/bin/bash

. bin/setenv.sh

if [ -z "$1" ] 
then
    echo "You must specify server"
    exit 1;
fi

SERVER=$1

# download and extract spark, prepare folder for project copy
ssh root@$SERVER "
    cd $WRK
    wget -q $URL_SPARK -O spark.tgz
    mkdir spark-bin-hadoop
    mkdir project
    mkdir project/target
    tar -xzvf spark.tgz -C spark-bin-hadoop --strip 1 > /dev/null
    rm spark.tgz
"
