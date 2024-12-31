#!/bin/bash

export DATA_PATH="${HOME}/data"
export LOG_PATH="${HOME}/log"
export MYSQL_REPLICA_SETUP_PATH="${HOME}/replica-setup-mysql"
export POSTGRES_REPLICA_SETUP_PATH="${HOME}/replica-setup-postgres"
export PID_FILE="${LOG_PATH}/myduck.pid"
export INIT_SQLS_DIR="/docker-entrypoint-initdb.d"

parse_dsn() {
    # Check if SOURCE_DSN is set
    if [ -z "$SOURCE_DSN" ]; then
        echo "Error: SOURCE_DSN environment variable is not set"
        exit 1
    fi

    local dsn="$SOURCE_DSN"

    # Initialize variables
    SOURCE_TYPE=""
    SOURCE_USER=""
    SOURCE_PASSWORD=""
    SOURCE_HOST=""
    SOURCE_PORT=""
    SOURCE_DATABASE=""

    # Detect type
    if [[ "$dsn" =~ ^postgres:// ]]; then
        SOURCE_TYPE="POSTGRES"
        # Strip the prefix
        dsn="${dsn#postgres://}"
    elif [[ "$dsn" =~ ^mysql:// ]]; then
        SOURCE_TYPE="MYSQL"
        # Strip the prefix
        dsn="${dsn#mysql://}"
    else
        echo "Error: Unsupported DSN format: the URI scheme must be 'postgres' or 'mysql'"
        exit 1
    fi

    # Extract credentials and host/port/dbname, stopping at any query parameters
    if [[ "$dsn" =~ ^([^:@]+)(:([^@]*))?@([^:/]+)(:([0-9]+))?(/([^?]+))? ]]; then
        export SOURCE_USER="${BASH_REMATCH[1]}"
        export SOURCE_PASSWORD="${BASH_REMATCH[3]}"
        export SOURCE_HOST="${BASH_REMATCH[4]}"
        export SOURCE_PORT="${BASH_REMATCH[6]}"
        export SOURCE_DATABASE="${BASH_REMATCH[8]}"

        # Remove the query parameters from SOURCE_DSN
        export SOURCE_DSN="${dsn%%\?*}"
    else
        echo "Error: Failed to parse DSN"
        exit 1
    fi

    # Handle empty SOURCE_DATABASE
    if [[ -z "$SOURCE_DATABASE" ]]; then
        if [[ "$SOURCE_TYPE" == "POSTGRES" ]]; then
            export SOURCE_DATABASE="postgres"
        elif [[ "$SOURCE_TYPE" == "MYSQL" ]]; then
            export SOURCE_DATABASE="mysql"
        fi
    fi

    # Set default ports if not specified
    if [[ -z "$SOURCE_PORT" ]]; then
        if [[ "$SOURCE_TYPE" == "POSTGRES" ]]; then
            export SOURCE_PORT="5432"
        elif [[ "$SOURCE_TYPE" == "MYSQL" ]]; then
            export SOURCE_PORT="3306"
        fi
    fi

    # Extract query parameters if present
    if [[ "$dsn" =~ \?(.+)$ ]]; then
        local query_string="${BASH_REMATCH[1]}"
        # Initialize filter variables
        local include_schemas=""
        local exclude_schemas=""
        local include_tables=""
        local exclude_tables=""
        
        # Parse query parameters
        IFS='&' read -ra PARAMS <<< "$query_string"
        for param in "${PARAMS[@]}"; do
            IFS='=' read -r key value <<< "$param"
            case "$key" in
                # Support both old and new parameter names
                "schemas"|"include-schemas") include_schemas="$value" ;;
                "exclude-schemas"|"skip-schemas") exclude_schemas="$value" ;;
                "tables"|"include-tables") include_tables="$value" ;;
                "exclude-tables"|"skip-tables") exclude_tables="$value" ;;
            esac
        done

        # Handle include-schemas from both path and query parameter
        if [[ -n "$SOURCE_DATABASE" && "$SOURCE_DATABASE" != "mysql" ]]; then
            if [[ -n "$include_schemas" ]]; then
                export INCLUDE_SCHEMAS="$SOURCE_DATABASE,$include_schemas"
            else
                export INCLUDE_SCHEMAS="$SOURCE_DATABASE"
            fi
        else
            export INCLUDE_SCHEMAS="$include_schemas"
        fi

        export EXCLUDE_SCHEMAS="$exclude_schemas"
        export INCLUDE_TABLES="$include_tables"
        export EXCLUDE_TABLES="$exclude_tables"
    else
        # If no query parameters, but SOURCE_DATABASE is set
        if [[ -n "$SOURCE_DATABASE" && "$SOURCE_DATABASE" != "mysql" ]]; then
            export INCLUDE_SCHEMAS="$SOURCE_DATABASE"
        fi
    fi

    echo "SOURCE_TYPE=$SOURCE_TYPE"
    echo "SOURCE_USER=$SOURCE_USER"
    echo "SOURCE_PASSWORD=$SOURCE_PASSWORD"
    echo "SOURCE_HOST=$SOURCE_HOST"
    echo "SOURCE_PORT=$SOURCE_PORT"
    echo "SOURCE_DATABASE=$SOURCE_DATABASE"

    # Exit if host is localhost, 127.0.0.1, 0.0.0.0 or ::1
    if [[ "$SOURCE_HOST" =~ ^localhost$|^127\.0\.0\.1$|^0\.0\.0\.0$|^::1$ ]]; then
        echo "Error: SOURCE_HOST cannot be $SOURCE_HOST when running in Docker."
        echo "Please use host.docker.internal for connecting to the host machine."
        echo "In addition, if you are on Linux, add the '--add-host=host.docker.internal:host-gateway' option to the 'docker run' command."
        exit 1
    fi
}

# Add signal handling function
cleanup() {
    echo "Received shutdown signal, cleaning up..."
    if [[ -f "${PID_FILE}" ]]; then
        kill "$(cat "${PID_FILE}")" 2>/dev/null
        rm -f "${PID_FILE}"
    fi
}

# Define MYSQL_PASSWORD_OPTION based on SUPERUSER_PASSWORD
if [ -z "$SUPERUSER_PASSWORD" ]; then
    MYSQL_PASSWORD_OPTION="--no-password"
else
    MYSQL_PASSWORD_OPTION="--password=$SUPERUSER_PASSWORD"
fi

# Function to run replica setup
run_replica_setup() {
    case "$SOURCE_TYPE" in
        MYSQL)
            echo "Replicating MySQL primary server: DSN=$SOURCE_DSN ..."
            cd "$MYSQL_REPLICA_SETUP_PATH" || {
                echo "Error: Could not change directory to ${MYSQL_REPLICA_SETUP_PATH}";
                exit 1;
            }
            ;;
        POSTGRES)
            echo "Replicating PostgreSQL primary server: DSN=$SOURCE_DSN ..."
            cd "$POSTGRES_REPLICA_SETUP_PATH" || {
                echo "Error: Could not change directory to ${POSTGRES_REPLICA_SETUP_PATH}";
                exit 1;
            }
            ;;
        *)
            echo "Error: Invalid SOURCE_TYPE value: ${SOURCE_TYPE}. Valid options are: MYSQL, POSTGRES."
            exit 1
            ;;
    esac

    export MYDUCK_PASSWORD="${SUPERUSER_PASSWORD}"

    # Run replica_setup.sh and check for errors
    if source replica_setup.sh; then
        echo "Replica setup completed."
    else
        echo "Error: Replica setup failed."
        exit 1
    fi
}

run_server_in_background() {
      cd "$DATA_PATH" || { echo "Error: Could not change directory to ${DATA_PATH}"; exit 1; }
      nohup myduckserver \
        ${DEFAULT_DB_OPTION} \
        ${SUPERUSER_PASSWORD_OPTION} \
        ${LOG_LEVEL_OPTION} \
        ${PROFILER_PORT_OPTION} \
        ${RESTORE_FILE_OPTION} \
        ${RESTORE_ENDPOINT_OPTION} \
        ${RESTORE_ACCESS_KEY_ID_OPTION} \
        ${RESTORE_SECRET_ACCESS_KEY_OPTION} \
        | tee -a "${LOG_PATH}/server.log" 2>&1 &
      echo "$!" > "${PID_FILE}"
}

wait_for_my_duck_server_ready() {
    local host="127.0.0.1"
    local user="root"
    local port="3306"
    local max_attempts=30
    local attempt=0
    local wait_time=2

    echo "Waiting for MyDuck Server at $host:$port to be ready..."

    until mysqlsh --sql --host "$host" --port "$port" --user "$user" ${MYSQL_PASSWORD_OPTION} --execute "SELECT VERSION();" &> /dev/null; do
        attempt=$((attempt+1))
        if [ "$attempt" -ge "$max_attempts" ]; then
            echo "Error: MySQL connection timeout after $max_attempts attempts."
            exit 1
        fi
        echo "Attempt $attempt/$max_attempts: MyDuck Server is unavailable - retrying in $wait_time seconds..."
        sleep $wait_time
    done

    echo "MyDuck Server is ready!"
}


# Function to check if a process is alive by its PID file
check_process_alive() {
    local pid_file="$1"
    local proc_name="$2"

    if [[ -f "${pid_file}" ]]; then
        local pid
        pid=$(<"${pid_file}")

        if [[ -n "${pid}" && -e "/proc/${pid}" ]]; then
            return 0  # Process is running
        else
            echo "${proc_name} (PID: ${pid}) is not running."
            return 1
        fi
    else
        echo "PID file for ${proc_name} not found!"
        return 1
    fi
}

execute_init_sqls() {
    local host="127.0.0.1"
    local mysql_user="root"
    local mysql_port="3306"
    local postgres_user="postgres"
    local postgres_port="5432"
    if [ -d "$INIT_SQLS_DIR/mysql" ] && [ "$(find "$INIT_SQLS_DIR/mysql" -maxdepth 1 -name '*.sql' -type f | head -n 1)" ]; then
        echo "Executing init SQL scripts from $INIT_SQLS_DIR/mysql..."
        for file in "$INIT_SQLS_DIR/mysql"/*.sql; do
            echo "Executing $file..."
            mysqlsh --sql --host "$host" --port "$mysql_port" --user "$mysql_user" $MYSQL_PASSWORD_OPTION --file="$file"
        done
    fi
    if [ -d "$INIT_SQLS_DIR/postgres" ] && [ "$(find "$INIT_SQLS_DIR/postgres" -maxdepth 1 -name '*.sql' -type f | head -n 1)" ]; then
        echo "Executing init SQL scripts from $INIT_SQLS_DIR/postgres..."
        for file in "$INIT_SQLS_DIR/postgres"/*.sql; do
            echo "Executing $file..."
            PGPASSWORD="$SUPERUSER_PASSWORD" psql -h "$host" -p "$postgres_port" -U "$postgres_user" -f "$file"
        done
    fi
}

# Handle the setup_mode
setup() {
    # Setup signal handlers
    trap cleanup SIGTERM SIGINT SIGQUIT

    if [ -n "$DEFAULT_DB" ]; then
        export DEFAULT_DB_OPTION="--default-db=$DEFAULT_DB"
    fi

    if [ -n "$SUPERUSER_PASSWORD" ]; then
        export SUPERUSER_PASSWORD_OPTION="--superuser-password=$SUPERUSER_PASSWORD"
    fi

    if [ -n "$LOG_LEVEL" ]; then
        export LOG_LEVEL_OPTION="--loglevel=$LOG_LEVEL"
    fi
    
    if [ -n "$PROFILER_PORT" ]; then
        export PROFILER_PORT_OPTION="--profiler-port=$PROFILER_PORT"
    fi

    if [ -n "$RESTORE_FILE" ]; then
        export RESTORE_FILE_OPTION="--restore-file=$RESTORE_FILE"
    fi

    if [ -n "$RESTORE_ENDPOINT" ]; then
        export RESTORE_ENDPOINT_OPTION="--restore-endpoint=$RESTORE_ENDPOINT"
    fi

    if [ -n "$RESTORE_ACCESS_KEY_ID" ]; then
        export RESTORE_ACCESS_KEY_ID_OPTION="--restore-access-key-id=$RESTORE_ACCESS_KEY_ID"
    fi

    if [ -n "$RESTORE_SECRET_ACCESS_KEY" ]; then
        export RESTORE_SECRET_ACCESS_KEY_OPTION="--restore-secret-access-key=$RESTORE_SECRET_ACCESS_KEY"
    fi

    # Ensure required directories exist
    mkdir -p "${DATA_PATH}" "${LOG_PATH}"

    case "$SETUP_MODE" in
        "" | "SERVER")
            echo "Starting MyDuck Server in SERVER mode..."
            run_server_in_background
            wait_for_my_duck_server_ready
            execute_init_sqls
            ;;
        "REPLICA")
            echo "Starting MyDuck Server in REPLICA mode..."
            parse_dsn
            run_server_in_background
            wait_for_my_duck_server_ready
            execute_init_sqls
            run_replica_setup
            ;;
        *)
            echo "Error: Invalid SETUP_MODE value. Valid options are: SERVER, REPLICA."
            exit 1
            ;;
    esac
}

setup

while true; do
    # Check if the processes have started
    if ! check_process_alive "$PID_FILE" "MyDuck Server"; then
        echo "CRITICAL: MyDuck Server process died unexpectedly."
        cleanup
        exit 1
    fi

    # Sleep before the next status check
    sleep 10
done
