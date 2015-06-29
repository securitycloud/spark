#!/bin/bash

. bin/setenv.sh

INSTALLED_SLAVES=0

# start start a standalone master server at the first machine
echo starting standalone spark master server at ${ALL_SERVERS[1]}, url: $MASTERURL

ssh root@${ALL_SERVERS[1]} "
   $WRK/spark-bin-hadoop/sbin/start-master.sh
"
# start workers on all the other machines
for i in "${ALL_SERVERS[@]}"
do
	if [ "${i}" != "${ALL_SERVERS[1]}" ] && [ "$INSTALLED_SLAVES" -lt "$NUMBER_OF_SLAVES" ]
	then
		ssh root@${i} "
			echo starting slave node at ${i}
			$WRK/spark-bin-hadoop/bin/spark-class org.apache.spark.deploy.worker.Worker $MASTERURL > /dev/null 2>&1 &
		"
		let "INSTALLED_SLAVES += 1"
	fi
done
