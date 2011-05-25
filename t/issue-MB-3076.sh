#!/bin/bash

ret=0

echo STATS testing ------------------------------

echo starting moxi...

./moxi -d -P /tmp/moxi-3076-test-moxi.pid \
  -z 11266=127.0.0.1:11277,127.0.0.1:11288 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=100,wait_queue_timeout=100

sleep 1

count=$(echo stats | nc 127.0.0.1 11266 | grep "STAT" | wc -l)
if [[ $count -ne 0 ]] ; then
    echo "FAIL expected no STAT's"
    ret=1
else
    echo "OK expect no STAT's"
fi

echo starting memcached simulant 11277...

./moxi -d -P /tmp/moxi-3076-test-memcached.pid -p 11277

sleep 1

count=$(echo stats | nc 127.0.0.1 11266 | grep "STAT" | wc -l)
if [[ $count -eq 0 ]] ; then
    echo "FAIL expected some STAT's"
    ret=1
else
    echo "OK stat has STAT's"
fi

kill `cat /tmp/moxi-3076-test-memcached.pid`

echo starting memcached simulant 11288...

./moxi -d -P /tmp/moxi-3076-test-memcached.pid -p 11288

sleep 1

count=$(echo stats | nc 127.0.0.1 11266 | grep "STAT" | wc -l)
if [[ $count -eq 0 ]] ; then
    echo "FAIL expected some STAT's"
    ret=1
else
    echo "OK stat has STAT's"
fi

killall moxi

echo starting moxi...

./moxi -d -P /tmp/moxi-3076-test-moxi.pid \
  -z 11266=127.0.0.1:11277,127.0.0.1:11288 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=100,wait_queue_timeout=100

echo starting memcached simulant 11277...

./moxi -d -P /tmp/moxi-3076-test-memcached.pid -p 11277

sleep 1

count=$(echo stats | nc 127.0.0.1 11266 | grep "STAT" | wc -l)
if [[ $count -eq 0 ]] ; then
    echo "FAIL expected some STAT's"
    ret=1
else
    echo "OK stat has STAT's"
fi

killall moxi

echo starting moxi...

./moxi -d -P /tmp/moxi-3076-test-moxi.pid \
  -z 11266=127.0.0.1:11277,127.0.0.1:11288 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=100,wait_queue_timeout=100

echo starting memcached simulant 11288...

./moxi -d -P /tmp/moxi-3076-test-memcached.pid -p 11288

sleep 1

count=$(echo stats | nc 127.0.0.1 11266 | grep "STAT" | wc -l)
if [[ $count -eq 0 ]] ; then
    echo "FAIL expected some STAT's"
    ret=1
else
    echo "OK stat has STAT's"
fi

killall moxi

echo multiget testing ------------------------------

echo starting moxi...

./moxi -d -P /tmp/moxi-3076-test-moxi.pid \
  -z 11266=127.0.0.1:11277,127.0.0.1:11288 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=100,wait_queue_timeout=100

sleep 1

count=$(echo get a x | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 0 ]] ; then
    echo "FAIL expected no VALUE's"
    ret=1
else
    echo "OK expect no VALUE's"
fi

count=$(echo get x a | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 0 ]] ; then
    echo "FAIL expected no VALUE's"
    ret=1
else
    echo "OK expect no VALUE's"
fi

echo starting memcached simulant 11277...

./moxi -d -P /tmp/moxi-3076-test-memcached.pid -p 11277
sleep 1
echo -e "set a 0 0 1\r\na\r" | nc 127.0.0.1 11277
echo -e "set b 0 0 1\r\nb\r" | nc 127.0.0.1 11277

echo get a x | nc 127.0.0.1 11266
count=$(echo get a b | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected some VALUE's"
    ret=1
else
    echo "OK expected some VALUE's"
fi

count=$(echo get a b | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected some VALUE's"
    ret=1
else
    echo "OK expected some VALUE's"
fi

kill `cat /tmp/moxi-3076-test-memcached.pid`

echo starting memcached simulant 11288...

./moxi -d -P /tmp/moxi-3076-test-memcached.pid -p 11288
sleep 1
echo -e "set a 0 0 1\r\nA\r" | nc 127.0.0.1 11288
echo -e "set b 0 0 1\r\nB\r" | nc 127.0.0.1 11288

count=$(echo get a b | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected some VALUE's"
    ret=1
else
    echo "OK expected some VALUE's"
fi

count=$(echo get a b | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected some VALUE's"
    ret=1
else
    echo "OK expected some VALUE's"
fi

killall moxi

echo starting moxi...

./moxi -d -P /tmp/moxi-3076-test-moxi.pid \
  -z 11266=127.0.0.1:11277,127.0.0.1:11288 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=100,wait_queue_timeout=100

echo starting memcached simulant 11277...

./moxi -d -P /tmp/moxi-3076-test-memcached.pid -p 11277
sleep 1
echo -e "set a 0 0 1\r\na\r" | nc 127.0.0.1 11277
echo -e "set b 0 0 1\r\nb\r" | nc 127.0.0.1 11277

count=$(echo get a b | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected some VALUE's"
    ret=1
else
    echo "OK expected some VALUE's"
fi

count=$(echo get a b | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected some VALUE's"
    ret=1
else
    echo "OK expected some VALUE's"
fi

killall moxi

echo starting moxi...

./moxi -d -P /tmp/moxi-3076-test-moxi.pid \
  -z 11266=127.0.0.1:11277,127.0.0.1:11288 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=100,wait_queue_timeout=100

echo starting memcached simulant 11288...

./moxi -d -P /tmp/moxi-3076-test-memcached.pid -p 11288
sleep 1
echo -e "set a 0 0 1\r\nA\r" | nc 127.0.0.1 11288
echo -e "set b 0 0 1\r\nB\r" | nc 127.0.0.1 11288

count=$(echo get a b | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected some VALUE's"
    ret=1
else
    echo "OK expected some VALUE's"
fi

count=$(echo get a b | nc 127.0.0.1 11266 | grep "VALUE" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL expected some VALUE's"
    ret=1
else
    echo "OK expected some VALUE's"
fi

killall moxi

exit $ret
