#!/bin/bash

# Detect OS platform (Linux or Darwin)
OS=$(uname -s)

# Use the EXECUTED_GTID_SET variable from the previous steps
if [ $GTID_MODE == "ON" ] && [ ! -z "$EXECUTED_GTID_SET" ]; then
  mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT} --user=${MYDUCK_USER} ${MYDUCK_PASSWORD_OPTION} <<EOF
SET GLOBAL gtid_purged = "${EXECUTED_GTID_SET}";
EOF
fi

# Connect to MySQL and execute the replication configuration commands
REPLICATION_CMD="CHANGE REPLICATION SOURCE TO \
  SOURCE_HOST='${SOURCE_HOST}', \
  SOURCE_PORT=${SOURCE_PORT}, \
  SOURCE_USER='${SOURCE_USER}', \
  SOURCE_PASSWORD='${SOURCE_PASSWORD}'"

if [ $GTID_MODE == "OFF" ]; then
  REPLICATION_CMD="${REPLICATION_CMD}, \
  SOURCE_LOG_FILE='${BINLOG_FILE}', \
  SOURCE_LOG_POS=${BINLOG_POS}"
fi

mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT} --user=${MYDUCK_USER} ${MYDUCK_PASSWORD_OPTION} <<EOF
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
