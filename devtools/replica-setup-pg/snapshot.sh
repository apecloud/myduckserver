# cd "$DATA_PATH" || { echo "Error: Could not change directory to ${DATA_PATH}"; exit 1; }

#!/bin/bash

# Execute SQL commands in DuckDB
duckdb <<EOF
INSTALL postgres_scanner;
LOAD postgres_scanner;
.open mysql.db
ATTACH 'dbname=postgres user=postgres password=root host=127.0.0.1 port=15432' AS pg_postgres (TYPE POSTGRES);
BEGIN;
COPY FROM DATABASE pg_postgres TO mysql;
EOF

# Capture the result of SELECT into a variable
WAL_LSN=$(duckdb -c "SELECT * FROM postgres_query('pg_postgres', 'SELECT pg_current_wal_lsn()');")

# Commit the transaction
duckdb -c "COMMIT;"

# Print the captured WAL LSN
echo "Current WAL LSN: $WAL_LSN"