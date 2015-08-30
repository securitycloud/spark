#!/bin/bash

. bin/setenv.sh

# increase for one master node
let "NUMBER_OF_SLAVES += 1"
INSTALLED_SLAVES=0

# install spark on each machine into WRK directory
for i in "${ALL_SERVERS[@]}"
do
	# install the desired amount of slaves + one master
	if [ "$INSTALLED_SLAVES" -le "$NUMBER_OF_SLAVES"  ]
		then
			echo installing spark on $i
			. bin/install-spark.sh $i > /dev/null
			let "INSTALLED_SLAVES += 1"
		fi
done
