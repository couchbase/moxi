#!/bin/bash

ret=0

echo starting saslBucketsStreamingSimulant...

( cat << EOF
HTTP/1.0 200 OK

{"buckets":[]}




EOF
) | nc -l 22100 &

echo starting moxi...

./moxi -d -P /tmp/moxi-3113-test-moxi.pid \
  -z http://127.0.0.1:22100/saslBucketsStreamingSimulant -t 1 \
  -Z default_bucket_name=default,port_listen=11266

sleep 3

# Before the fix, these requests would incorrectly return...
#
#   SERVER_ERROR unauthorized, null bucket
#
# Instead, we want moxi to just close the upstream conn
# when there are no buckets, so that upstream conn's are
# not inadvertently stuck on null bucket.
#
echo -e "incr a 1\r\nincr a 2\r" | nc 127.0.0.1 11266

count=$(echo -e "incr a 1\r\nincr a 2\r" | nc 127.0.0.1 11266 | wc -l)
if [[ $count -ne 0 ]] ; then
    echo "FAIL expected closed conns (especially not '')"
    ret=1
else
    echo "OK got closed conns"
fi

killall moxi

exit $ret
