#!/bin/bash

. scripts/setenv.sh

for i in "${ALL_SERVERS[@]}"
do
	echo installing spark on $i
	. scripts/install-spark.sh $i
done
