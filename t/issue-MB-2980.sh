#!/bin/bash

ret=0

echo starting memcached simulant at 11277

./moxi -d -P /tmp/moxi-2980-test-memcached.pid -p 11277

echo starting moxi at 11266

./moxi -d -P /tmp/moxi-2980-test-moxi.pid \
  -z 11266=127.0.0.1:11277 \
  -Z downstream_protocol=ascii,port_listen=11266,front_cache_lifespan=3000,front_cache_max=1000,front_cache_spec=yes,front_cache_unspec=no,cycle=100

sleep 1

# Only yes* keys should be front-cached.  nah* and no* keys should not be front-cached.

echo -e "set yes-0 0 0 1\r\n1\r" | nc 127.0.0.1 11266
echo -e "set nah-0 0 0 1\r\n1\r" | nc 127.0.0.1 11266
echo -e "set no-0 0 0 1\r\n1\r" | nc 127.0.0.1 11266

count=$(echo get yes-0 | nc 127.0.0.1 11266 | grep "VALUE yes-0 0 1" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get yes-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get nah-0 | nc 127.0.0.1 11266 | grep "VALUE nah-0 0 1" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get nah-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get no-0 | nc 127.0.0.1 11266 | grep "VALUE no-0 0 1" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get no-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get yes-0 | nc 127.0.0.1 11277 | grep "VALUE yes-0 0 1" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get yes-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get nah-0 | nc 127.0.0.1 11277 | grep "VALUE nah-0 0 1" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get nah-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get no-0 | nc 127.0.0.1 11277 | grep "VALUE no-0 0 1" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get no-0"
    ret=1
else
    echo "OK get $count"
fi

# Test multiget while we're here.

count=$(echo get yes-0 nah-0 no-0 | nc 127.0.0.1 11266 | egrep "VALUE .*-0 0 1" | wc -l)
if [[ $count -ne 3 ]] ; then
    echo "FAIL multiget moxi"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get yes-0 nah-0 no-0 | nc 127.0.0.1 11277 | egrep "VALUE .*-0 0 1" | wc -l)
if [[ $count -ne 3 ]] ; then
    echo "FAIL multiget direct"
    ret=1
else
    echo "OK get $count"
fi


# Change some items directly, bypassing moxi.

echo -e "set yes-0 0 0 2\r\n22\r" | nc 127.0.0.1 11277
echo -e "set nah-0 0 0 2\r\n22\r" | nc 127.0.0.1 11277
echo -e "set no-0 0 0 2\r\n22\r" | nc 127.0.0.1 11277

count=$(echo get yes-0 | nc 127.0.0.1 11266 | grep "VALUE yes-0 0 1" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get yes-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get nah-0 | nc 127.0.0.1 11266 | grep "VALUE nah-0 0 2" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get nah-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get no-0 | nc 127.0.0.1 11266 | grep "VALUE no-0 0 2" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get no-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get yes-0 | nc 127.0.0.1 11277 | grep "VALUE yes-0 0 2" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get yes-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get nah-0 | nc 127.0.0.1 11277 | grep "VALUE nah-0 0 2" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get nah-0"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get no-0 | nc 127.0.0.1 11277 | grep "VALUE no-0 0 2" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "FAIL get no-0"
    ret=1
else
    echo "OK get $count"
fi

# Wait until front cache expires and check the items again.

sleep 3

count=$(echo get yes-0 nah-0 no-0 | nc 127.0.0.1 11266 | egrep "VALUE .*-0 0 2" | wc -l)
if [[ $count -ne 3 ]] ; then
    echo "FAIL multiget"
    ret=1
else
    echo "OK get $count"
fi

count=$(echo get yes-0 nah-0 no-0 | nc 127.0.0.1 11277 | egrep "VALUE .*-0 0 2" | wc -l)
if [[ $count -ne 3 ]] ; then
    echo "FAIL multiget"
    ret=1
else
    echo "OK get $count"
fi

killall moxi

if [[ $ret -ne 0 ]] ; then
    echo FAIL
else
    echo OK
fi

exit $ret
