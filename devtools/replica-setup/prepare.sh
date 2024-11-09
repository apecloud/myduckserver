#!/bin/bash

if ! mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT}  --user=root --no-password -e "SELECT 1 FROM mysql.user WHERE user = '${MYDUCK_USER}'" | grep -q 1; then
    echo "Creating user ${MYDUCK_USER} for replication and setting local_infile & server_id..."
    mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT}  --user=root --no-password <<EOF
CREATE USER '${MYDUCK_USER}'@'%' IDENTIFIED BY '${MYDUCK_PASSWORD}';
GRANT ALL PRIVILEGES ON *.* TO '${MYDUCK_USER}'@'%';
SET GLOBAL local_infile = 1;
SET GLOBAL server_id = ${MYDUCK_SERVER_ID};
EOF
fi

if [[ $? -ne 0 ]]; then
    echo "Failed to create user '${MYDUCK_USER}'  or set local_infile & server_id. Exiting."
    exit 1
fi