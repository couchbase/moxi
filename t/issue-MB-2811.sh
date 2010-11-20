#!/bin/sh

date > /tmp/issue-MB-2811.log

echo starting moxi

# Start moxi with high timeouts, especially SASL auth_timeout, so that
# a worker thread hangs during synchronous SASL auth.  With this
# configuration, we want to cause the main thread to (incorrectly)
# hang for 30 seconds (before the fix).

./moxi -d -P /tmp/moxi-2811-test-moxi.pid \
  -z http://127.0.0.1:22100/test \
  -Z port_listen=11266,downstream_conn_max=1,downstream_max=0,downstream_timeout=30000,wait_queue_timeout=30000,downstream_conn_queue_timeout=30000,connect_timeout=30000,auth_timeout=30000

echo starting memcached simulant

./moxi -d -P /tmp/moxi-2811-test-memcached.pid -p 11277

# Control our stdout at this point, as it goes to nc -l.
# First, stream to moxi one vbucket config.

(   cat << EOF
HTTP/1.0 200 OK

{"name":"default",
 "bucketType":"membase",
 "authType":"sasl",
 "saslPassword":"",
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
    (
        sleep 5
        date
        echo client request 0 - ensure moxi has started
        echo incr a 1 | nc 127.0.0.1 11266

        date
        echo stopping memcached simulant...
        kill -STOP `cat /tmp/moxi-2811-test-memcached.pid`
        sleep 1

        echo client request 1 - make 1 worker thread busy with auth
        time (echo incr a 3 | nc 127.0.0.1 11266) &
        sleep 1
        ) >> /tmp/issue-MB-2811.log

    # Stream another reconfig message, which will hang
    # the main thread while a worker thread is hung.

    cat << EOF
{"name":"default",
 "bucketType":"membase",
 "authType":"sasl",
 "saslPassword":"",
 "nodes":[
    {"replication":0.0,"clusterMembership":"active","status":"healthy",
     "hostname":"127.0.0.1:22100",
     "ports":{"proxy":11266,"direct":11277}}
 ],
 "nodeLocator":"vbucket",
 "vBucketServerMap":{
   "hashAlgorithm":"CRC","numReplicas":0,
   "serverList":["127.0.0.1:11277"],
   "vBucketMap":[[0],[0]]
  }
}



EOF
    (   echo client request 2 - incorrectly hangs for auth_timeout msecs
        time (echo version | nc 127.0.0.1 11266)
        ) >> /tmp/issue-MB-2811.log
) | nc -l 22100

echo OK - no more hanging
killall moxi

