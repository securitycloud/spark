#!/bin/bash

. bin/setenv.sh

# kill all java processes on all machines
for i in "${ALL_SERVERS[@]}"
do
	ssh root@$i " 
		killall java
		"
done

