#!/bin/bash

mkdir /tmp/log

docker run -t -d --ulimit nofile=50000:50000  -d -v /sys/fs/cgroup:/sys/fs/cgroup:ro -v /tmp/log:/tmp/log --cpus=4 --privileged --cap-add=NET_ADMIN --cap-add=NET_RAW --hostname edge-1 --name edge1 graphos

