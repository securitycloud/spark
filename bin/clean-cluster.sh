#!/bin/bash

. bin/setenv.sh
. bin/kill-cluster.sh

# empty the work directory on each machine
for i in "${ALL_SERVERS[@]}"
do
	echo clearing work directory on $i
	ssh root@$i "
		rm -rf $WRK/*
		"
done
