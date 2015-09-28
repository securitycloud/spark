#!/bin/bash

. bin/setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify Test type $OFF
    exit 1;
fi
TESTTYPE=$1

if [ -z "$2" ] 
then
    echo -e $ERR You must specify Number of computers $OFF
    exit 2;
fi
COMPUTERS=$2

if [ -z "$3" ] 
then
    echo -e $ERR You must specify Number of partitions $OFF
    exit 3;
fi
PARTITIONS=$3

if [ -z "$4" ] 
then
    echo -e $ERR You must specify Batch size $OFF
    exit 4;
fi
BATCH_SIZE=$4

echo -e $LOG Running Test $TESTTYPE on $COMPUTERS computers $OFF

# pack and copy the spark project
tar -cf project.tar src pom.xml
scp project.tar root@${ALL_SERVERS[1]}:/$WRK
rm project.tar
SERVERS=${ALL_SERVERS[@]}
# compile and run, then scp to all slave nodes
NUMBER_OF_SLAVES=$((COMPUTERS - 1))
# copy to master node and from there to all slave nodes
ssh root@${ALL_SERVERS[1]} "
	cd ${WRK}
	rm -rf project/
	mkdir project
	tar -xf project.tar -C project
	cd project
	mvn clean package > /dev/null
       if [ "$?" -gt 0 ]
    	then
        	exit 1;
    	fi
	if [ "$NUMBER_OF_SLAVES" -ge 1 ]
		then
			echo copying slave node to ${ALL_SERVERS[2]}
			scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[2]}:$WRK/project/target
		fi
	if [ "$NUMBER_OF_SLAVES" -ge 2 ]
		then
    		echo copying slave node to ${ALL_SERVERS[3]}
    		scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[3]}:$WRK/project/target
    	fi
    if [ "$NUMBER_OF_SLAVES" -ge 3 ]
    	then
        	echo copying slave node to ${ALL_SERVERS[4]}
            scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[4]}:$WRK/project/target
    	fi
    if [ "$NUMBER_OF_SLAVES" -ge 4 ]
    	then
        	echo copying slave node to ${ALL_SERVERS[5]}
            scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[5]}:$WRK/project/target
    	fi
	mvn exec:exec -Dspark.machines=$COMPUTERS -Dspark.testtype=$TESTTYPE
"
echo -e ${OK} Test in progress... cluster monitoring at http://${ALL_SERVERS[1]}:8080
# wait for one message to SERVICE TOPIC to signal test done
ssh root@${KAFKA_CONSUMER} "
	 $KAFKA_INSTALL/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ${SERVICE_TOPIC} --max-messages 1
"
# restart cluster
echo -e ${OFF} Restarting cluster
bin/kill-cluster.sh
bin/start-cluster.sh > /dev/null