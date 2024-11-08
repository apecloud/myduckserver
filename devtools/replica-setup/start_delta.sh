#!/bin/bash

if [[ $SOURCE_IS_EMPTY -eq 0 ]]; then
  SQL="SHOW BINARY LOG STATUS\G"
  if [ "$MYSQL_VERSION_IS_LESS_THAN_8_3" -eq 0 ]; then
    SQL="SHOW MASTER STATUS;"
  fi
  EXECUTED_GTID_SET=$(mysqlsh --host="$MYSQL_HOST" --user="$MYSQL_USER" --password="$MYSQL_PASSWORD" --sql -e "$SQL" | grep -i "Executed_Gtid_Set" | awk -F': ' '{print $2}')
fi

# Use the EXECUTED_GTID_SET variable from the previous steps
if [ -z "$EXECUTED_GTID_SET" ]; then
  echo "Executed_GTID_set is empty, exiting."
  exit 1
fi

# Connect to MySQL and execute the replication configuration commands
mysqlsh --sql --host=127.0.0.1 --user=root --port=3306 --password='' <<EOF
SET global gtid_purged = "${EXECUTED_GTID_SET}";
CHANGE REPLICATION SOURCE TO SOURCE_HOST='${MYSQL_HOST}',
  SOURCE_PORT=${MYSQL_PORT},
  SOURCE_USER='${MYSQL_USER}',
  SOURCE_PASSWORD='${MYSQL_PASSWORD}'
;
START REPLICA;
EOF

# Check if the commands were successful
if [ $? -ne 0 ]; then
  echo "Failed to start replication. Exiting."
  exit 1
else
  echo "Replication started successfully."
fi