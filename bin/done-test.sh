#!/bin/bash

. bin/setenv.sh

# LOG
echo -e $LOG Waiting for finish test $OFF

# UNTIL TEST HAS BEEN DONE
while [ true ]
do
    DONE=false

    bin/result-download.sh > /tmp/done-test
    tac /tmp/done-test > /tmp/done-test-revert
    while read LINE
    do
        DONE=true
        break;
    done < /tmp/done-test-revert
    rm /tmp/done-test /tmp/done-test-revert

    if [ "$DONE" = "true" ]; then break; fi
    sleep 10
done

echo -e $LOG Test finished $OFF
