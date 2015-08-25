#!/bin/bash

. bin/setenv.sh

# pack and copy the spark project
tar -cf project.tar src pom.xml
scp project.tar ${ALL_SERVERS[1]}:/$WRK
rm project.tar
SERVERS=${ALL_SERVERS[@]}
# compile and run, then scp to all slave nodes
ssh ${ALL_SERVERS[1]} "
	cd ${WRK}
	rm -rf project
	mkdir project
	tar -xf project.tar -C project
	cd project
	mvn clean package
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
    if [ "$NUMBER_OF_SLAVES" -ge 5  ]
        then
                echo copying slave node to ${ALL_SERVERS[6]}
            scp target/sparkTest-1.0-SNAPSHOT-jar-with-dependencies.jar ${ALL_SERVERS[6]}:$WRK/project/target
        fi

	mvn exec:exec
"
