#!/bin/bash

echo "Setting local_infile and server_id..."
mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT}  --user=${MYDUCK_USER} ${MYDUCK_PASSWORD_OPTION} <<EOF
SET GLOBAL local_infile = 1;
SET GLOBAL server_id = ${MYDUCK_SERVER_ID};
SET GLOBAL replica_is_loading_snapshot = ON;
EOF
