cd "$DATA_PATH" || { echo "Error: Could not change directory to ${DATA_PATH}"; exit 1; }

# Execute the DuckDB SQL command and capture the output
POSTGRES_DSN="'dbname=postgres user=${SOURCE_USER} password=${SOURCE_PASSWORD} host=${SOURCE_HOST} port=${SOURCE_PORT}'"

WAL_LSN=$(duckdb <<EOF | awk '/│.*│/{gsub(/[ │]/, "", $0); print; exit}'
INSTALL postgres_scanner;
LOAD postgres_scanner;
.open mysql.db
ATTACH "${POSTGRES_DSN}" AS pg_postgres (TYPE POSTGRES);
BEGIN;
COPY FROM DATABASE pg_postgres TO mysql;
SELECT * FROM postgres_query('pg_postgres', 'SELECT pg_current_wal_lsn()');
COMMIT;
EOF
)

# Validate and print WAL_LSN
if [[ -z "$WAL_LSN" ]]; then
    echo "Error: Failed to retrieve WAL LSN."
    exit 1
else
    echo "Current WAL LSN: $WAL_LSN"
fi