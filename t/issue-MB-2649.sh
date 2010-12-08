#!/bin/bash

ret=0

if [ ! -f ../labrea/labrea ] ; then
    echo SKIPPED - require labrea
    exit 0
fi

killall moxi || true

echo "
-- Slow down reads
function before_read(...)
   labrea.usleep(200000)
end
" > /tmp/slow-node.lua

echo starting slow memcached simulant 11277

../labrea/labrea /tmp/slow-node.lua \
  ./moxi -d -P /tmp/moxi-slow-node-test-memcached0.pid -p 11277

sleep 1

echo -e "set a 0 0 1\r\na\r" | nc 127.0.0.1 11277

echo starting moxi...

./moxi -d -P /tmp/moxi-slow-node-test-moxi.pid \
  -z 11266=127.0.0.1:11277 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_conn_queue_timeout=100,wait_queue_timeout=0

sleep 1

echo 1 request to take up the 1 downstream conn
echo get a | nc 127.0.0.1 11266 > /tmp/slow-node-0.out &

echo expect timeout on next requests
echo get a | nc 127.0.0.1 11266 > /tmp/slow-node-1.out &

echo expect timeout on next requests
echo get a | nc 127.0.0.1 11266 > /tmp/slow-node-2.out &

sleep 1

count=$(grep "VALUE" /tmp/slow-node-0.out | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected VALUE"
    ret=1
else
    echo "OK expected VALUE"
fi

count=$(grep "VALUE" /tmp/slow-node-1.out | wc -l)
if [[ $count -ne 0 ]] ; then
    echo "FAIL expected no VALUE"
    ret=1
else
    echo "OK expected no VALUE"
fi

count=$(grep "VALUE" /tmp/slow-node-2.out | wc -l)
if [[ $count -ne 0 ]] ; then
    echo "FAIL expected no VALUE"
    ret=1
else
    echo "OK expected no VALUE"
fi

sleep 1

count=$(echo get a | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected VALUE"
    ret=1
else
    echo "OK expected VALUE after downstream_conn_queue timeouts"
fi

killall moxi

exit $ret
