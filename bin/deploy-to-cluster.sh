#!/bin/bash

. bin/setenv.sh

#PACK AND COPY
tar -cf project.tar src pom.xml
scp project.tar root@${ALL_SERVERS[1]}:/$WRK
rm project.tar

#COMPILE AND RUN
ssh root@${ALL_SERVERS[1]} "
	cd ${WRK}
	rm -rf project
	mkdir project
	tar -xf project.tar -C project
	cd project
	mvn clean package -Ptestbed
	if [ "$?" -gt 0 ]
    	then
        	exit 1;
    	fi
	scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[2]}:$WRK/project/target
	scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[3]}:$WRK/project/target
	scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[4]}:$WRK/project/target
	scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar root@${ALL_SERVERS[5]}:$WRK/project/target
	mvn exec:exec -Ptestbed
"
