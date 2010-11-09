#!/bin/sh

echo starting moxi...

# Note that changing downstream_conn_max to 0 (correctly) does not hang.

./moxi -d -P /tmp/moxi-dcm-test-moxi.pid \
  -z 11266=127.0.0.1:11277 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=5000,wait_queue_timeout=5000,downstream_retry=1 -t 1

echo starting memcached simulant...

./moxi -d -P /tmp/moxi-dcm-test-memcached.pid -p 11277

echo client request 1 - expect NOT_FOUND

time (echo incr a 1 | nc 127.0.0.1 11266)

echo killing memcached simulant...

kill `cat /tmp/moxi-dcm-test-memcached.pid`

echo client request 2 - hangs in bug MB-2660, instead expect SERVER_ERROR

time (echo incr a 1 | nc 127.0.0.1 11266)

echo client request 3 - hangs in one fix attempt, instead expect SERVER_ERROR

time (echo incr a 1 | nc 127.0.0.1 11266)

echo client request 4 - expect SERVER_ERROR

time (echo incr a 1 | nc 127.0.0.1 11266)

echo OK - no more hanging

killall moxi

