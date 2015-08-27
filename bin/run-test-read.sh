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

#recreating of topic
#echo -e $LOG Recreating output topic $TESTING_TOPIC with 1 partitions on $KAFKA_CONSUMER $OFF
#bin/run-topic.sh $TESTING_TOPIC 1 $KAFKA_CONSUMER

#echo -e $LOG Logging info to service topic: $SERVICE_TOPIC $OFF
#ssh $KAFKA_CONSUMER "
#    echo Type=read, Filter=$FILTER, Computers=$COMPUTERS, Partitions=$PARTITIONS, BatchSize=$BATCH_SIZE |
#        $KAFKA_INSTALL/bin/kafka-console-producer.sh --topic $SERVICE_TOPIC --broker-list localhost:9092
#"

echo -e $LOG Running Test $TESTTYPE on $COMPUTERS computers


# pack and copy the spark project
tar -cf project.tar src pom.xml
scp project.tar ${ALL_SERVERS[1]}:/$WRK
rm project.tar
SERVERS=${ALL_SERVERS[@]}
# compile and run, then scp to all slave nodes
NUMBER_OF_SLAVES=$((COMPUTERS - 1))

ssh ${ALL_SERVERS[1]} "
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
	if [ "$NUMBER_OF_SLAVES" -ge 1  ]
		then
			echo copying slave node to ${ALL_SERVERS[2]}
			scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar ${ALL_SERVERS[2]}:$WRK/project/target
		fi
	if [ "$NUMBER_OF_SLAVES" -ge 2  ]
    	then
    		echo copying slave node to ${ALL_SERVERS[3]}
    		scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar ${ALL_SERVERS[3]}:$WRK/project/target
    	fi
    if [ "$NUMBER_OF_SLAVES" -ge 3  ]
        then
        	echo copying slave node to ${ALL_SERVERS[4]}
            scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar ${ALL_SERVERS[4]}:$WRK/project/target
        fi
    if [ "$NUMBER_OF_SLAVES" -ge 4  ]
        then
        	echo copying slave node to ${ALL_SERVERS[5]}
            scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar ${ALL_SERVERS[5]}:$WRK/project/target
        fi
	mvn exec:exec -Dspark.machines=$COMPUTERS -Dspark.testtype=$TESTTYPE | sed -n -e 's/^.*Driver successfully submitted as //p' > /tmp/driverId.txt
"

# wait for one message to signal test done
$KAFKA_INSTALL/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ${SERVICE_TOPIC} --max-messages 1
#sleep 120

ssh ${ALL_SERVERS[1]} '
    driverid=$(</tmp/driverId.txt)
    /home/securitycloud/spark/spark-bin-hadoop/bin/spark-class org.apache.spark.deploy.Client kill spark://sc1:7077 \${driverid}
'
