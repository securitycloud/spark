#!/bin/bash

. bin/setenv.sh

# install spark on each machine into WRK directory
for i in "${ALL_SERVERS[@]}"
do
	echo installing spark on $i
	. bin/install-spark.sh $i
done
