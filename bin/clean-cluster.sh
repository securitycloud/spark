#!/bin/bash

. bin/setenv.sh

. bin/kill-cluster.sh

for i in "${ALL_SERVERS[@]}"
do
	echo clearing on $i
	ssh root@$i "
		echo removing spark
		rm -rf $WRK/*
		"
done
