#!/bin/bash

. scripts/setenv.sh

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
	mvn exec:exec -Ptestbed
"
