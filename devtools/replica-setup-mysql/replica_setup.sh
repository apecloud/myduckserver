#!/bin/bash

usage() {
    echo "Usage: $0 --mysql_host <host> --mysql_port <port> --mysql_user <user> --mysql_password <password> [--myduck_host <host>] [--myduck_port <port>] [--myduck_user <user>] [--myduck_password <password>]"
    exit 1
}

MYDUCK_HOST=${MYDUCK_HOST:-127.0.0.1}
MYDUCK_PORT=${MYDUCK_PORT:-3306}
MYDUCK_USER=${MYDUCK_USER:-root}
MYDUCK_PASSWORD=${MYDUCK_PASSWORD:-}
MYDUCK_SERVER_ID=${MYDUCK_SERVER_ID:-2}
GTID_MODE="ON"
GTID_EXECUTED=""
SOURCE_IS_MARIADB="false"

while [[ $# -gt 0 ]]; do
    case $1 in
        --mysql_host)
            SOURCE_HOST="$2"
            shift 2
            ;;
        --mysql_port)
            SOURCE_PORT="$2"
            shift 2
            ;;
        --mysql_user)
            SOURCE_USER="$2"
            shift 2
            ;;
        --mysql_password)
            SOURCE_PASSWORD="$2"
            shift 2
            ;;
        --myduck_host)
            MYDUCK_HOST="$2"
            shift 2
            ;;
        --myduck_port)
            MYDUCK_PORT="$2"
            shift 2
            ;;
        --myduck_user)
            MYDUCK_USER="$2"
            shift 2
            ;;
        --myduck_password)
            MYDUCK_PASSWORD="$2"
            shift 2
            ;;
        --myduck_server_id)
            MYDUCK_SERVER_ID="$2"
            shift 2
            ;;
        *)
            echo "Unknown parameter: $1"
            usage
            ;;
    esac
done

# if SOURCE_PASSWORD is empty, set SOURCE_PASSWORD_OPTION to "--no-password"
if [[ -z "$SOURCE_PASSWORD" ]]; then
    SOURCE_PASSWORD_OPTION="--no-password"
else
    SOURCE_PASSWORD_OPTION=""
fi

# if MYDUCK_PASSWORD is empty, set MYDUCK_PASSWORD_OPTION to "--no-password"
if [[ -z "$MYDUCK_PASSWORD" ]]; then
    MYDUCK_PASSWORD_OPTION="--no-password"
else
    MYDUCK_PASSWORD_OPTION="--password=$MYDUCK_PASSWORD"
fi

# Check if all parameters are set
if [[ -z "$SOURCE_HOST" || -z "$SOURCE_PORT" || -z "$SOURCE_USER" ]]; then
    echo "Error: Missing required MySQL connection variables: SOURCE_HOST, SOURCE_PORT, SOURCE_USER."
    usage
fi

source checker.sh

# A snapshot normally creates the target databases before replication starts.
# Sources that do not support MySQL Shell's copy-instance (for example Dolt)
# skip that step, but their existing CREATE DATABASE statements are already
# behind the replication start position. Create the DSN target schema
# explicitly so the first replicated table DDL has a database to attach to.
initialize_target_schema() {
    local schema="${SOURCE_DATABASE:-}"
    if [[ -z "$schema" || "$schema" == "mysql" ]]; then
        echo "No explicit source database configured; skipping target schema initialization."
        return 0
    fi

    # Keep the DSN database compatible with SQL identifier syntax. A backtick
    # cannot be represented safely by this shell SQL wrapper, so reject it.
    local escaped_schema
    if [[ "$schema" == *'`'* ]]; then
        echo "Source database contains an unsupported backtick: $schema" >&2
        return 1
    fi
    escaped_schema="$schema"
    echo "Initializing target schema from SOURCE_DATABASE=$SOURCE_DATABASE..."
    mysqlsh --sql --host="${MYDUCK_HOST}" --port="${MYDUCK_PORT}" \
        --user="${MYDUCK_USER}" "${MYDUCK_PASSWORD_OPTION}" <<< \
        "CREATE DATABASE IF NOT EXISTS \`${escaped_schema}\`;"
}

# Step 1: Check if mysqlsh exists, if not, install it
if ! command -v mysqlsh &> /dev/null; then
    echo "mysqlsh not found, attempting to install..."
    bash install_mysql_shell.sh
    check_command "mysqlsh installation"
else
    echo "mysqlsh is already installed."
fi

# Step 2: Check if replication has already been started
echo "Checking if replication has already been started..."
check_if_myduck_has_replica
if [[ $? -ne 0 ]]; then
    echo "Replication has already been started. Exiting."
    exit 1
fi

# Step 3: Check MySQL configuration
echo "Checking MySQL configuration..."
check_mysql_config
check_command "MySQL configuration check"

# Step 3: Prepare MyDuck Server for replication
echo "Preparing MyDuck Server for replication..."
source prepare.sh
check_command "preparing MyDuck Server for replication"

# Step 4: Copy the existing data from the source MySQL instance to MyDuck Server
echo "Checking if source server supports MySQL Shell..."
if check_if_source_supports_copying_instance; then
    echo "Copying a snapshot of the MySQL instance to MyDuck Server..."
    source snapshot.sh
    check_command "copying a snapshot of the MySQL instance"
else
    echo "The source server cannot be copied using MySQL Shell. The snapshot step has been skipped."
    # Without a snapshot, existing source tables and rows cannot be copied to
    # the empty target. Reject that case instead of advancing past their DDL
    # and silently starting a healthy replica with missing data.
    if [[ -n "${SOURCE_DATABASE:-}" && "$SOURCE_DATABASE" != "mysql" ]]; then
        if [[ "$SOURCE_DATABASE" == *'`'* ]]; then
            echo "Source database contains an unsupported backtick: $SOURCE_DATABASE" >&2
            exit 1
        fi
        source_database_sql=$(printf '%s' "$SOURCE_DATABASE" | sed "s/'/''/g")
        existing_tables_output=$(mysqlsh --uri="$SOURCE_DSN" $SOURCE_PASSWORD_OPTION --sql \
            --result-format=tabbed -e \
            "SELECT COUNT(*) AS table_count FROM information_schema.tables WHERE table_schema='${source_database_sql}';")
        check_command "checking source tables before snapshotless replication"
        existing_tables=$(printf '%s\n' "$existing_tables_output" \
            | awk 'NF && $1 ~ /^[0-9]+$/ { value=$1 } END { if (value == "") exit 1; print value }')
        check_command "parsing source table count before snapshotless replication"
        if [[ "$existing_tables" != "0" ]]; then
            echo "Source database '$SOURCE_DATABASE' already has $existing_tables table(s); snapshotless replication requires an empty source database." >&2
            exit 1
        fi
    fi

    # The source position is the replication baseline after the empty-source
    # check. This prevents pre-setup CREATE DATABASE from being replayed after
    # the empty target schema is initialized below.
    EXECUTED_GTID_SET="$GTID_EXECUTED"
    echo "Using source GTID position as replication baseline: $EXECUTED_GTID_SET"
fi

# A skipped snapshot leaves the target without the source databases. This is
# idempotent after a successful snapshot and is required before START REPLICA
# when the source's CREATE DATABASE event predates the replication position.
echo "Initializing target schema before replication..."
initialize_target_schema
check_command "initializing target schemas"

# Step 5: Establish replication
echo "Starting replication..."
source start_replication.sh
check_command "starting replication"
