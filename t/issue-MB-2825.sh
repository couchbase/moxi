#!/bin/sh

echo starting moxi...

# Note, if you zero'ed the timeouts, moxi would also
# (unsurprisingly) hang at client request 3.

./moxi -d -P /tmp/moxi-2825-test-moxi.pid \
  -z 11266=127.0.0.1:11277 -t 1 \
  -Z downstream_conn_max=1,downstream_max=0,downstream_timeout=0,wait_queue_timeout=0,downstream_conn_queue_timeout=100

echo starting memcached simulant...

./moxi -d -P /tmp/moxi-2825-test-memcached.pid -p 11277

echo client request 1 - expect NOT_FOUND
time (echo incr a 1 | nc 127.0.0.1 11266)

echo stopping memcached simulant...
kill -STOP `cat /tmp/moxi-2825-test-memcached.pid`

echo client request 2 - use up the one downstream conn
time (echo incr a 2 | nc 127.0.0.1 11266) &

sleep 1

echo client request 3 - hangs in 2825, with the fix expect SERVER_ERROR
time (echo incr a 3 | nc 127.0.0.1 11266)

echo client request 4 - hangs in 2825, with the fix expect SERVER_ERROR
time (echo incr a 4 | nc 127.0.0.1 11266)

echo OK - no more hanging
killall moxi

