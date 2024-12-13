#!/bin/bash

if ! mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT}  --user=root --no-password -e "SELECT 1 FROM mysql.user WHERE user = '${MYDUCK_USER}'" | grep -q 1; then
    echo "Creating user ${MYDUCK_USER} for replication..."
    mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT}  --user=root --no-password <<EOF
CREATE USER '${MYDUCK_USER}'@'%' IDENTIFIED BY '${MYDUCK_PASSWORD}';
GRANT ALL PRIVILEGES ON *.* TO '${MYDUCK_USER}'@'%';
EOF
fi

if [[ $? -ne 0 ]]; then
    echo "Failed to create user '${MYDUCK_USER}'. Exiting."
    exit 1
fi

echo "Setting local_infile and server_id..."
mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT}  --user=root --no-password <<EOF
SET GLOBAL local_infile = 1;
SET GLOBAL server_id = ${MYDUCK_SERVER_ID};
SET GLOBAL replica_is_loading_snapshot = ON;
EOF
