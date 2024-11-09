#!/bin/bash

# Detect OS platform (Linux or Darwin)
OS=$(uname -s)

if [[ $SOURCE_IS_EMPTY -eq 0 ]]; then
  EXECUTED_GTID_SET=$(mysqlsh --host="$MYSQL_HOST" --user="$MYSQL_USER" --password="$MYSQL_PASSWORD" --sql -e "SHOW BINARY LOG STATUS\G" | grep -i "Executed_Gtid_Set" | awk -F': ' '{print $2}')
  if [[ -z "$EXECUTED_GTID_SET" ]]; then
    echo "Failed to get executed GTID set by statement 'SHOW BINARY LOG STATUS\G'. Trying to get it by statement 'SHOW MASTER STATUS\G'..."
    EXECUTED_GTID_SET=$(mysqlsh --host="$MYSQL_HOST" --user="$MYSQL_USER" --password="$MYSQL_PASSWORD" --sql -e "SHOW MASTER STATUS\G" | grep -i "Executed_Gtid_Set" | awk -F': ' '{print $2}')
  fi
fi

# Use the EXECUTED_GTID_SET variable from the previous steps
if [ -z "$EXECUTED_GTID_SET" ]; then
  echo "Executed_GTID_set is empty, exiting."
  exit 1
fi

if [[ "${MYDUCK_IN_DOCKER}" =~ "true" ]] && [[ "$OS" == "Darwin" ]] && ([[ "${MYSQL_HOST}" == "127.0.0.1" ]] || [[ "${MYSQL_HOST}" == "localhost" ]] || [[ "${MYSQL_HOST}" == "0.0.0.0" ]]); then
    MYSQL_HOST_FOR_REPLICA="host.docker.internal"
else
    MYSQL_HOST_FOR_REPLICA="${MYSQL_HOST}"
fi

# Connect to MySQL and execute the replication configuration commands
mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT} --user=root --no-password <<EOF
SET global gtid_purged = "${EXECUTED_GTID_SET}";
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='${MYSQL_HOST_FOR_REPLICA}',
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
  echo "Replication established successfully."
fi