#!/bin/bash

set -eu

cd "${BASH_SOURCE%/*}/.."

# use unstable for Thrift 0.19.0
docker run -v "${PWD}/starrocks-thrift-sdk:/starrocks-thrift-sdk" --rm debian:unstable /bin/sh -c "\
set -eux
apt-get update -q
apt-get install -q -y wget automake bison flex g++ git libboost-all-dev libevent-dev libssl-dev libtool make pkg-config
cd /starrocks-thrift-sdk/
./build-thrift-with-docker.sh
"