#!/bin/bash
################################################################################
# Copyright 2013-2017 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

nodes=${1:-1}
home=$(pwd)

function wait_for_node {
  log=$1
  if [ ! -f $log ]
  then
    echo "A log file does not exist at $(pwd)/${log}"
    exit 1
  fi

  i=0
  while [ $i -le 12 ]
  do
    sleep 1
    grep -i "there will be cake" ${log}
    if [ $? == 0 ]; then
      return 0
    else
      i=$(($i + 1))
      echo -n "."
    fi
  done
  echo "The cake is a lie!"
  tail -n 1000 ${log}
  exit 2
}

function start_server {
  instance=$1
  dir="instance${instance}"
  port=$((2900 + 100 * $instance))
  mkdir ${dir}
  ./bin/aerospike init --home ${dir} --instance ${instance} --service-port ${port}
  cd ${dir}
  ./bin/aerospike start
  wait_for_node "var/log/aerospike.log"
  cd ..
}

function install_server {
  wget -nv -O aerospike-server.tgz http://aerospike.com/download/server/latest/artifact/tgz
  tar xzf aerospike-server.tgz
  cp -f .travis/aerospike.conf ./aerospike-server/share/etc
  cd aerospike-server
  sed -i -e 's/\${me}/"root"/' share/libexec/aerospike-start
  sed -i -e 's/set_shmmax$/#set_shmmax/' share/libexec/aerospike-start
  sed -i -e 's/set_shmall$/#set_shmall/' share/libexec/aerospike-start
  sed -i -e 's/set_socket_buffer_limits$/#set_socket_buffer_limits/' share/libexec/aerospike-start
  sed -i -e 's/ulimit/#ulimit/' share/libexec/aerospike-start
}

echo "## Fetching Aerospike server distribution"
install_server
for ((node = 1; node <= nodes; node++))
do
  echo "## Starting Aerospike server instance ${node}/${nodes}"
  start_server $node
done
cd ${pwd}
