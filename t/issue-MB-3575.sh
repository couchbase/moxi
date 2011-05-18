#!/bin/sh

rm -f /tmp/issue-MB-3575.out*

( cat << EOF
HTTP/1.0 200 OK

This is just a bad response that is not JSON.
EOF
) | nc -l 22100 &

( cat << EOF
{ "buckets": [
{"name":"default",
 "foo":"bar",
 "saslPassword": "",
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
] }
EOF
) > /tmp/issue-MB-3575-good.cfg

ruby ./t/rest_mock.rb /tmp/issue-MB-3575-good.cfg &

sleep 1

echo starting moxi

./moxi \
  -z http://127.0.0.1:22100/bad,http://127.0.0.1:4567/pools/default/bucketsStreaming/default \
  -Z port_listen=11266,downstream_conn_max=1,downstream_max=0,downstream_timeout=300,wait_queue_timeout=300,downstream_conn_queue_timeout=300,connect_timeout=300,auth_timeout=300 2>> /tmp/issue-MB-3575.out &

sleep 1

echo OK

killall moxi
killall nc

ps | grep ruby | grep rest_mock.rb | cut -d ' ' -f 1 - | xargs kill -KILL

echo --------------- >> /tmp/issue-MB-3575.out

( cat << EOF
HTTP/1.0 200 OK

This is just a bad response that is not JSON.
EOF
) | nc -l 22100 &

( cat << EOF
{ "buckets": [
{"name":"default",
 "foo":"bar",
 "saslPassword": "",
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
] }
EOF
) > /tmp/issue-MB-3575-good.cfg

ruby ./t/rest_mock.rb /tmp/issue-MB-3575-good.cfg &

sleep 1

echo starting moxi

./moxi \
  -z http://127.0.0.1:4567/pools/default/bucketsStreaming/default,http://127.0.0.1:22100/bad \
  -Z port_listen=11266,downstream_conn_max=1,downstream_max=0,downstream_timeout=300,wait_queue_timeout=300,downstream_conn_queue_timeout=300,connect_timeout=300,auth_timeout=300 2>> /tmp/issue-MB-3575.out &

sleep 1

echo OK

killall moxi
killall nc

ps | grep ruby | grep rest_mock.rb | cut -d ' ' -f 1 - | xargs kill -KILL

echo --------------- >> /tmp/issue-MB-3575.out

( cat << EOF
HTTP/1.0 200 OK

This is just a bad response that is not JSON.
EOF
) | nc -l 22100 &

( cat << EOF
HTTP/1.0 200 OK

This is just a bad response that is not JSON.
EOF
) | nc -l 22101 &

( cat << EOF
{ "buckets": [
{"name":"default",
 "foo":"bar",
 "saslPassword": "",
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
] }
EOF
) > /tmp/issue-MB-3575-good.cfg

ruby ./t/rest_mock.rb /tmp/issue-MB-3575-good.cfg &

sleep 1

echo starting moxi

./moxi \
  -z http://127.0.0.1:22100/bad,http://127.0.0.1:22101/bad,http://127.0.0.1:4567/pools/default/bucketsStreaming/default \
  -Z port_listen=11266,downstream_conn_max=1,downstream_max=0,downstream_timeout=300,wait_queue_timeout=300,downstream_conn_queue_timeout=300,connect_timeout=300,auth_timeout=300 2>> /tmp/issue-MB-3575.out &

sleep 1

echo OK

killall moxi
killall nc

ps | grep ruby | grep rest_mock.rb | cut -d ' ' -f 1 - | xargs kill -KILL

echo --------------- >> /tmp/issue-MB-3575.out

( cat << EOF
HTTP/1.0 200 OK

This is just a bad response that is not JSON.
EOF
) | nc -l 22100 &

( cat << EOF
HTTP/1.0 200 OK

This is just a bad response that is not JSON.
EOF
) | nc -l 22101 &

( cat << EOF
{ "buckets": [
{"name":"default",
 "foo":"bar",
 "saslPassword": "",
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
] }
EOF
) > /tmp/issue-MB-3575-good.cfg

ruby ./t/rest_mock.rb /tmp/issue-MB-3575-good.cfg &

sleep 1

echo starting moxi

./moxi \
  -z http://127.0.0.1:22100/bad,http://127.0.0.1:4567/pools/default/bucketsStreaming/default,http://127.0.0.1:22101/bad \
  -Z port_listen=11266,downstream_conn_max=1,downstream_max=0,downstream_timeout=300,wait_queue_timeout=300,downstream_conn_queue_timeout=300,connect_timeout=300,auth_timeout=300 2>> /tmp/issue-MB-3575.out &

sleep 1

echo OK

killall moxi
killall nc

ps | grep ruby | grep rest_mock.rb | cut -d ' ' -f 1 - | xargs kill -KILL

echo ----------------------

cut -d ' ' -f 4- /tmp/issue-MB-3575.out > /tmp/issue-MB-3575.out2

count=$( (diff /tmp/issue-MB-3575.out2 - <<EOF
ERROR: parse JSON failed, from REST server: http://127.0.0.1:22100/bad, This is just a bad response that is not JSON.

ERROR: invalid, empty config from REST server http://127.0.0.1:4567/pools/default/bucketsStreaming/default
---------------
ERROR: invalid, empty config from REST server http://127.0.0.1:4567/pools/default/bucketsStreaming/default
---------------
ERROR: parse JSON failed, from REST server: http://127.0.0.1:22100/bad, This is just a bad response that is not JSON.

ERROR: parse JSON failed, from REST server: http://127.0.0.1:22101/bad, This is just a bad response that is not JSON.

ERROR: invalid, empty config from REST server http://127.0.0.1:4567/pools/default/bucketsStreaming/default
---------------
ERROR: parse JSON failed, from REST server: http://127.0.0.1:22100/bad, This is just a bad response that is not JSON.

ERROR: invalid, empty config from REST server http://127.0.0.1:4567/pools/default/bucketsStreaming/default
EOF
) | wc -l)

if [[ $count -ne 0 ]] ; then
    echo "FAIL count expect 0, got $count"
    exit 1
fi

echo
echo "OK - issue-MB-3575 fixed"


