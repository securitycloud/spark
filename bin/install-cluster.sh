#!/bin/bash

. bin/setenv.sh

for i in "${ALL_SERVERS[@]}"
do
	echo installing spark on $i
	. bin/install-spark.sh $i
done
