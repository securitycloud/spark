#!/bin/bash

. bin/setenv.sh
. bin/kill-cluster.sh

# empty the work directory on each machine
for i in "${ALL_SERVERS[@]}"
do
	echo clearing on $i
	ssh $i "
		echo removing spark
		rm -rf $WRK/*
		"
done
