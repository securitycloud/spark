#!/bin/bash

. bin/setenv.sh

# start start a standalone master server at the first machine
echo starting standalone spark master server at ${ALL_SERVERS[1]}, url: $MASTERURL

ssh root@${ALL_SERVERS[1]} "
   $WRK/spark-bin-hadoop/sbin/start-master.sh
"
# start workers on all the other machines
for i in "${ALL_SERVERS[@]}"
do
	if [ "${i}" != "${ALL_SERVERS[1]}" ]
	then
		ssh root@${i} "
			echo starting slave node at ${i}
			$WRK/spark-bin-hadoop/bin/spark-class org.apache.spark.deploy.worker.Worker $MASTERURL > /dev/null 2>&1 &
		"
	fi
done
