#!/bin/bash

# Detect OS platform (Linux or Darwin for macOS)
OS=$(uname -s)

LOCAL_HOST="127.0.0.1"

if [[ "$OS" == "Darwin" ]]; then
    LOCAL_HOST="host.docker.internal"
fi

# mkdir for mysql
mkdir -p htap/mysql

# mkdir for myduck
mkdir -p htap/duck

# mkdir for maxscale
mkdir -p maxscale

# create file my-maxscale.cnf
cat <<EOF > maxscale/my-maxscale.cnf
# Define the primary server
[server1]
type=server
address=${LOCAL_HOST}
port=3306
protocol=MariaDBBackend

# Define the secondary server
[server2]
type=server
address=${LOCAL_HOST}
port=3307
protocol=MariaDBBackend

# Monitor to check the status of the servers
[MySQL-Monitor]
type=monitor
module=mariadbmon
servers=server1,server2
user=root
password=''
monitor_interval=2000ms
assume_unique_hostnames=false

# The read-write splitting service
[Read-Write-Service]
type=service
router=readwritesplit
servers=server1,server2
user=root
password=''
filters=Hint

# The filter of hint
[Hint]
type=filter
module=hintfilter

# Define listener for the service
[Read-Write-Listener]
type=listener
service=Read-Write-Service
protocol=MariaDBClient
port=4000
EOF