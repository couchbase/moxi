#!/bin/sh

echo starting memcached simulant

./moxi -d -P /tmp/moxi-2972-test-memcached.pid -p 11277

( cat << EOF
HTTP/1.0 200 OK

{"name":"",
 "bucketType":"membase",
 "nodes":[
    {"replication":0.0,"clusterMembership":"active","status":"healthy",
     "hostname":"127.0.0.1:22100",
     "ports":{"proxy":11266,"direct":11277}}
 ],
 "nodeLocator":"vbucket",
 "vBucketServerMap":{
   "hashAlgorithm":"CRC","numReplicas":0,
   "serverList":["127.0.0.1:11277"],
   "vBucketMap":[[0]]
  }
}
EOF
) | nc -l 22100 &

echo starting moxi

./moxi -d -P /tmp/moxi-2972-test-moxi.pid \
  -z http://127.0.0.1:22100/test \
  -Z port_listen=11266,downstream_conn_max=1,downstream_max=0,downstream_timeout=300,wait_queue_timeout=300,downstream_conn_queue_timeout=300,connect_timeout=300,auth_timeout=300

sleep 1

date
echo client request 0 - ensure moxi has started
echo incr a 0 | nc 127.0.0.1 11266

date
echo stopping memcached simulant...
kill -STOP `cat /tmp/moxi-2972-test-memcached.pid`
sleep 1

echo client request 1 - attempt get when memcached is down
echo get 1 | nc 127.0.0.1 11266 > /tmp/moxi-2972.out
if ! (echo "SERVER_ERROR proxy downstream timeout\r" | diff - /tmp/moxi-2972.out); then \
    echo FAIL - did not get expected SERVER_ERROR
    killall moxi
    exit -1
fi

echo OK - got the expected SERVER_ERROR
killall moxi

echo ----------------------------------

( cat << EOF
HTTP/1.0 200 OK

{"name":"",
 "bucketType":"membase",
 "nodes":[
    {"replication":0.0,"clusterMembership":"active","status":"healthy",
     "hostname":"127.0.0.1:22100",
     "ports":{"proxy":11266,"direct":11277}}
 ],
 "nodeLocator":"vbucket",
 "vBucketServerMap":{
   "hashAlgorithm":"CRC","numReplicas":0,
   "serverList":["127.0.0.1:11277"],
   "vBucketMap":[[0]]
  }
}
EOF
) | nc -l 22100 &

echo starting moxi

./moxi -d -P /tmp/moxi-2972-test-moxi.pid \
  -z http://127.0.0.1:22100/test \
  -Z port_listen=11266,downstream_conn_max=1,downstream_max=0,downstream_timeout=300,wait_queue_timeout=300,downstream_conn_queue_timeout=300,connect_timeout=300,auth_timeout=300

sleep 1

echo client request 2 - attempt get when memcached is down
echo get 1 | nc 127.0.0.1 11266 > /tmp/moxi-2972.out
if ! (echo "SERVER_ERROR proxy write to downstream 127.0.0.1\r" | diff - /tmp/moxi-2972.out); then \
    echo FAIL - did not get expected SERVER_ERROR
    killall moxi
    exit -1
fi

echo OK - got the expected SERVER_ERROR
killall moxi

echo ----------------------------------

echo starting moxi

./moxi -d -P /tmp/moxi-2972-test-moxi.pid \
  -z 11266=127.0.0.1:11277 \
  -Z port_listen=11266,downstream_conn_max=1,downstream_max=0,downstream_timeout=300,wait_queue_timeout=300,downstream_conn_queue_timeout=300,connect_timeout=300,auth_timeout=300

sleep 1

echo client request 1 - attempt get when memcached is down
echo get 1 | nc 127.0.0.1 11266 > /tmp/moxi-2972.out
if ! (echo "END\r" | diff - /tmp/moxi-2972.out); then \
    echo FAIL - did not get expected END
    killall moxi
    exit -1
fi

echo OK - got the expected END
killall moxi

