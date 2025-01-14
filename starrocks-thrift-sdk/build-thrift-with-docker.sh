#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PARALLEL=$[$(nproc)/4+1]
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz"
THRIFT_NAME=thrift-0.13.0.tar.gz
THRIFT_SOURCE=thrift-0.13.0
THRIFT_MD5SUM="38a27d391a2b03214b444cb13d5664f1"

THRIFT_HOME=thrift
THRIFT_PKG=$THRIFT_HOME/$THRIFT_NAME

if [ -f "$THRIFT_PKG" ]
then
    echo "$THRIFT_PKG found."
else
    wget -P $THRIFT_HOME $THRIFT_DOWNLOAD
fi

md5sum_func() {
    echo 'check md5'
    local FILENAME=$1
    local DESC_DIR=$2
    local MD5SUM=$3

    gen_md5=`md5sum "$DESC_DIR/$FILENAME"`
    if [ "$gen_md5" = "$MD5SUM  $THRIFT_PKG" ]; then
        echo 'check done'
    else
        echo -e "except-md5 $MD5SUM \nactual-md5 $gen_md5"
        exit 1
    fi
}

md5sum_func $THRIFT_NAME $THRIFT_HOME $THRIFT_MD5SUM

cd $THRIFT_HOME


if [ -d "$THRIFT_SOURCE" ]
then
    echo "$THRIFT_SOURCE found."
else
    tar -zxvf $THRIFT_NAME
fi


ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`
echo ${ROOT}

TP_LIB_DIR=$ROOT/lib
TP_INCLUDE_DIR=$ROOT/incloud
TP_INSTALL_DIR=$ROOT/install

build_thrift() {
    cd $THRIFT_SOURCE
    ./configure CPPFLAGS="-I${TP_INCLUDE_DIR}" LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" LIBS="-lcrypto -ldl -lssl" CFLAGS="-fPIC" \
    --prefix=$TP_INSTALL_DIR --docdir=$TP_INSTALL_DIR/doc --enable-static --disable-shared --disable-tests \
    --disable-tutorial --without-qt4 --without-qt5 --without-csharp --without-erlang --without-nodejs \
    --without-lua --without-perl --without-php --without-php_extension --without-dart --without-ruby \
    --without-haskell --without-go --without-haxe --without-d --without-python -without-java --with-cpp \
    --with-libevent=$TP_INSTALL_DIR --with-boost=$TP_INSTALL_DIR --with-openssl=$TP_INSTALL_DIR

    if [ -f compiler/cpp/thrifty.hh ];then
        mv compiler/cpp/thrifty.hh compiler/cpp/thrifty.h
    fi
    make -j$PARALLEL && make install
}

if [ ! -f "$TP_INSTALL_DIR/bin/thrift" ]; then
    build_thrift
fi

cd $TP_INSTALL_DIR/bin/
TARGET=/starrocks-thrift-sdk/target/generated-sources/thrift
rm -rf $TARGET
mkdir -p $TARGET
./thrift -r -gen java $ROOT/../gensrc/StarrocksExternalService.thrift
mv gen-java/com $TARGET/
rm -rf thrift
echo "done..."