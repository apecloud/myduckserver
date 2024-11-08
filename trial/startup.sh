#!/bin/bash

# prepare files
/bin/bash prepare_configurations.sh

# docker-compose up
docker-compose up -d

# wait for mysql to be ready
echo "Waiting for mysql to be ready..."
while true; do
  docker exec myduck-mysql bash -c "mysql -h127.0.0.1 -P3306 -uroot -e 'select 1'"
  if [[ $? -eq 0 ]]; then
    break
  fi
  sleep 1
done

# change the server_id of myduckserver to 2
docker exec myduckserver bash -c "mysqlsh --sql --host=host.docker.internal --port=3307 --user=root --password='' -e 'set global server_id = 2'"

# setup replication stream
docker exec myduckserver bash -c "cd /home/admin/replica-setup; /bin/bash replica_setup.sh --mysql_host host.docker.internal --mysql_port 3306 --mysql_user root --mysql_password '' --myduckserver_host host.docker.internal --myduckserver_port 3307"

# create a user on primary and grant all privileges
docker exec myduck-mysql bash -c "mysql -h127.0.0.1 -P3306 -uroot -e \"create user 'lol'@'%' identified with 'mysql_native_password' by 'lol'; grant all privileges on *.* to 'lol'@'%';\""

# wait for myduckserver to replicate the user
echo "Waiting for myduckserver to replicate the user..."
while true; do
  USER_RET=$(docker exec myduckserver bash -c "mysqlsh --sql --host=host.docker.internal --port=3307 --user=root --password='' -e 'select user from mysql.user where user = \"lol\";'")
  if [[ -n $USER_RET ]]; then
    break
  fi
  sleep 1
done

# TODO(sean): This is a temporary workaround due to this bug: https://github.com/apecloud/myduckserver/issues/134
# alter user on myduckserver
docker exec myduckserver bash -c "mysqlsh --sql --host=host.docker.internal --port=3307 --user=root --password='' -e \"alter user 'lol'@'%' identified by 'lol';\""
