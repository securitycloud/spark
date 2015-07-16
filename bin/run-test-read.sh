#!/bin/bash

. bin/setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify Number of computers $OFF
    exit 1;
fi
COMPUTERS=$1

if [ -z "$2" ] 
then
    echo -e $ERR You must specify Number of partitions $OFF
    exit 2;
fi
PARTITIONS=$2

if [ -z "$3" ] 
then
    echo -e $ERR You must specify Batch size $OFF
    exit 3;
fi
BATCH_SIZE=$3

if [ -z "$4" ] 
then
    FILTER=""
    #echo -e $ERR You must specify Filter $OFF
    #exit 4;
else
    FILTER=$4
fi


echo -e $LOG Recreating output topic $TESTING_TOPIC with 1 partitions on $KAFKA_CONSUMER $OFF
bin/run-topic.sh $TESTING_TOPIC 1 $KAFKA_CONSUMER

#echo -e $LOG Logging info to service topic: $SERVICE_TOPIC $OFF
#ssh root@$KAFKA_CONSUMER "
#    echo Type=read, Filter=$FILTER, Computers=$COMPUTERS, Partitions=$PARTITIONS, BatchSize=$BATCH_SIZE |
#        $KAFKA_INSTALL/bin/kafka-console-producer.sh --topic $SERVICE_TOPIC --broker-list localhost:9092
#"

echo -e $LOG Running on $COMPUTERS computers, filter $FILTER $OFF


# pack and copy the spark project
tar -cf project.tar src pom.xml
scp project.tar root@${ALL_SERVERS[1]}:/$WRK
rm project.tar
SERVERS=${ALL_SERVERS[@]}
# compile and run, then scp to all slave nodes
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
	if [ "$NUMBER_OF_SLAVES" -ge 1  ]
		then
			echo copying slave node to ${ALL_SERVERS[2]}
			scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[2]}:$WRK/project/target
		fi
	if [ "$NUMBER_OF_SLAVES" -ge 2  ]
    	then
    		echo copying slave node to ${ALL_SERVERS[3]}
    		scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[3]}:$WRK/project/target
    	fi
    if [ "$NUMBER_OF_SLAVES" -ge 3  ]
        then
        	echo copying slave node to ${ALL_SERVERS[4]}
            scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[4]}:$WRK/project/target
        fi
    if [ "$NUMBER_OF_SLAVES" -ge 4  ]
        then
        	echo copying slave node to ${ALL_SERVERS[5]}
            scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[5]}:$WRK/project/target
        fi
	mvn exec:exec -Dspark.machines=$COMPUTERS -Dspark.filter=$FILTER | sed -n -e 's/^.*Driver successfully submitted as //p' > /root/spark/driverId.txt
"



sleep 120

ssh root@${ALL_SERVERS[1]} "
	cd ${WRK}
        DRIVERID=$(</root/spark/driverId.txt)
        $WRK/spark-bin-hadoop/bin/spark-class org.apache.spark.deploy.Client kill spark://sc-211:7077 \${DRIVERID}
"
