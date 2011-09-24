#!/bin/sh

echo starting moxi...

./moxi -d -P /tmp/moxi-conn-close-moxi.pid \
  -z 11266=127.0.0.1:11277 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=0,wait_queue_timeout=0,downstream_conn_queue_timeout=100,retry=0

echo starting memcached simulant...
./moxi -d -P /tmp/moxi-conn-close-memcached.pid -p 11277
sleep 2;

echo client request 1 - expect NOT_FOUND
echo incr a 1 | nc 127.0.0.1 11266

echo stopping memcached simulant...
kill -9 `cat /tmp/moxi-conn-close-memcached.pid`

echo starting again memcached simulant...
./moxi -d -p 11277
sleep 2

echo client request 2 - use up the one downstream conn. since that downstream connection is closed, \
and retry is zero, in the old moxi, it should have returned the downstream close error. But with the conn-close fix \
response will be NOT_FOUND.
out=`echo incr a 2 | nc 127.0.0.1 11266`

if [[ "$out" != NOT_FOUND* ]];then
echo "Test case failed"
else
echo "Test case passed"
fi

echo stopping moxi...
killall moxi

