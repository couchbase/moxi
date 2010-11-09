#!/bin/sh

echo starting moxi...

# Note, if you removed or zero'ed the timeouts, moxi would also
# (correctly, expectedly) hang at client request 2.

./moxi -d -P /tmp/moxi-2689-test-moxi.pid \
  -z 11266=127.0.0.1:11277 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=100,wait_queue_timeout=100

echo starting memcached simulant...

./moxi -d -P /tmp/moxi-2689-test-memcached.pid -p 11277

echo client request 1 - expect NOT_FOUND
time (echo incr a 1 | nc 127.0.0.1 11266)

echo stopping memcached simulant...
kill -STOP `cat /tmp/moxi-2689-test-memcached.pid`

echo client request 2 - hangs in bug MB-2689, instead expect SERVER_ERROR
time (echo incr a 1 | nc 127.0.0.1 11266)

echo client request 3 - expect SERVER_ERROR
time (echo incr a 1 | nc 127.0.0.1 11266)

echo client request 4 - expect SERVER_ERROR
time (echo incr a 1 | nc 127.0.0.1 11266)

echo OK - no more hanging
killall moxi

