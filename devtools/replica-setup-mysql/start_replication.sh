#!/bin/bash

# Detect OS platform (Linux or Darwin)
OS=$(uname -s)

# if [[ $SOURCE_IS_EMPTY -eq 0 ]]; then
#   EXECUTED_GTID_SET=$(mysqlsh --host="$SOURCE_HOST" --user="$SOURCE_USER" --password="$SOURCE_PASSWORD" --sql -e "SHOW BINARY LOG STATUS\G" | grep -i "Executed_Gtid_Set" | awk -F': ' '{print $2}')
#   if [[ -z "$EXECUTED_GTID_SET" ]]; then
#     echo "Failed to get executed GTID set by statement 'SHOW BINARY LOG STATUS\G'. Trying to get it by statement 'SHOW MASTER STATUS\G'..."
#     EXECUTED_GTID_SET=$(mysqlsh --host="$SOURCE_HOST" --user="$SOURCE_USER" --password="$SOURCE_PASSWORD" --sql -e "SHOW MASTER STATUS\G" | grep -i "Executed_Gtid_Set" | awk -F': ' '{print $2}')
#   fi
# fi

if [[ "${MYDUCK_IN_DOCKER}" == "true" && "$OS" == "Darwin" &&
      ("${SOURCE_HOST}" == "127.0.0.1" || "${SOURCE_HOST}" == "localhost" || "${SOURCE_HOST}" == "0.0.0.0") ]]; then
    SOURCE_HOST_FOR_REPLICA="host.docker.internal"
else
    SOURCE_HOST_FOR_REPLICA="${SOURCE_HOST}"
fi

# Use the EXECUTED_GTID_SET variable from the previous steps
if [ $GTID_MODE == "ON" ] && [ ! -z "$EXECUTED_GTID_SET" ]; then
  mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT} --user=root --no-password <<EOF
SET GLOBAL gtid_purged = "${EXECUTED_GTID_SET}";
EOF
fi

# Connect to MySQL and execute the replication configuration commands
REPLICATION_CMD="CHANGE REPLICATION SOURCE TO \
  SOURCE_HOST='${SOURCE_HOST_FOR_REPLICA}', \
  SOURCE_PORT=${SOURCE_PORT}, \
  SOURCE_USER='${SOURCE_USER}', \
  SOURCE_PASSWORD='${SOURCE_PASSWORD}'"

if [ $GTID_MODE == "OFF" ]; then
  REPLICATION_CMD="${REPLICATION_CMD}, \
  SOURCE_LOG_FILE='${BINLOG_FILE}', \
  SOURCE_LOG_POS=${BINLOG_POS}"
fi

mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT} --user=root --no-password <<EOF
${REPLICATION_CMD};
START REPLICA;
EOF

# Check if the commands were successful
if [ $? -ne 0 ]; then
  echo "Failed to start replication. Exiting."
  exit 1
else
  echo "Replication established successfully."
fi