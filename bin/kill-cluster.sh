#!/bin/bash

. bin/setenv.sh

for i in "${ALL_SERVERS[@]}"
do
	ssh root@$i "
		$WRK/spark-bin-hadoop/sbin/stop-all.sh 
		killall java
		"
done

