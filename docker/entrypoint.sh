#!/bin/bash

export DATA_PATH="${HOME}/data"
export LOG_PATH="${HOME}/log"
export MYSQL_REPLICA_SETUP_PATH="${HOME}/replica-setup-mysql"
export POSTGRES_REPLICA_SETUP_PATH="${HOME}/replica-setup-postgres"
export PID_FILE="${LOG_PATH}/myduck.pid"
export LOG_PIPE="${LOG_PATH}/myduck.log.pipe"
export INIT_SQLS_DIR="/docker-entrypoint-initdb.d"

SERVER_PID=""
LOGGER_PID=""
SETUP_PID=""
SERVER_SIGNAL_SENT=false
SETUP_SIGNAL_SENT=false
LOG_PIPE_GUARD_OPEN=false
PID_TEMP_FILE="${PID_FILE}.tmp.$$"
SHUTDOWN_SIGNAL=""
SERVER_OPTIONS=()

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

cleanup_process_files() {
    close_log_pipe_guard
    rm -f "${PID_FILE}" "${PID_TEMP_FILE}" "${LOG_PIPE}"
}

close_log_pipe_guard() {
    if [[ "${LOG_PIPE_GUARD_OPEN}" == "true" ]]; then
        exec 9>&-
        LOG_PIPE_GUARD_OPEN=false
    fi
}

signal_exit_status() {
    case "$1" in
        HUP) return 129 ;;
        INT) return 130 ;;
        QUIT) return 131 ;;
        TERM) return 143 ;;
        *) return 1 ;;
    esac
}

signal_children() {
    if [[ -z "${SHUTDOWN_SIGNAL}" ]]; then
        return
    fi
    if [[ "${SERVER_SIGNAL_SENT}" == "false" ]] && [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        if kill -s "${SHUTDOWN_SIGNAL}" "${SERVER_PID}" 2>/dev/null; then
            SERVER_SIGNAL_SENT=true
        fi
    fi
    if [[ "${SETUP_SIGNAL_SENT}" == "false" ]] && [[ -n "${SETUP_PID}" ]] && kill -0 "${SETUP_PID}" 2>/dev/null; then
        if kill -s TERM "${SETUP_PID}" 2>/dev/null; then
            SETUP_SIGNAL_SENT=true
        fi
    fi
}

# Signal delivery interrupts Bash's current wait. Record the request and signal
# every known child immediately; the main flow performs the blocking reap.
# shellcheck disable=SC2329
request_shutdown() {
    if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
        return
    fi
    SHUTDOWN_SIGNAL="$1"
    echo "Received ${SHUTDOWN_SIGNAL}, forwarding it to MyDuck Server..."
    signal_children
}

wait_for_pid() {
    local pid="$1"
    local status

    while true; do
        wait "${pid}" 2>/dev/null
        status=$?
        if kill -0 "${pid}" 2>/dev/null; then
            continue
        fi
        return "${status}"
    done
}

stop_setup_process() {
    local setup_pid="${SETUP_PID}"

    if [[ -z "${setup_pid}" ]]; then
        return
    fi
    if [[ "${SETUP_SIGNAL_SENT}" == "false" ]] && kill -0 "${setup_pid}" 2>/dev/null; then
        if kill -s TERM "${setup_pid}" 2>/dev/null; then
            SETUP_SIGNAL_SENT=true
        fi
    fi
    # The database has already drained at every call site. Do not let a
    # TERM-resistant readiness/init client keep PID 1 alive until Docker KILLs it.
    if kill -0 "${setup_pid}" 2>/dev/null; then
        kill -s KILL "${setup_pid}" 2>/dev/null
    fi
    wait_for_pid "${setup_pid}" >/dev/null 2>&1
    SETUP_PID=""
    SETUP_SIGNAL_SENT=false
}

run_setup_command() {
    local status

    if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
        complete_shutdown
    fi
    "$@" &
    SETUP_PID=$!
    SETUP_SIGNAL_SENT=false
    signal_children
    wait "${SETUP_PID}" 2>/dev/null
    status=$?
    if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
        complete_shutdown
    fi
    while kill -0 "${SETUP_PID}" 2>/dev/null; do
        wait "${SETUP_PID}" 2>/dev/null
        status=$?
    done
    SETUP_PID=""
    SETUP_SIGNAL_SENT=false
    return "${status}"
}

wait_for_server_and_logger() {
    local server_status=0
    local logger_status=0

    if [[ -n "${SERVER_PID}" ]]; then
        wait_for_pid "${SERVER_PID}"
        server_status=$?
        SERVER_PID=""
        SERVER_SIGNAL_SENT=false
    fi

    if [[ -n "${LOGGER_PID}" ]]; then
        wait_for_pid "${LOGGER_PID}"
        logger_status=$?
        LOGGER_PID=""
    fi

    if [[ ${server_status} -ne 0 ]]; then
        return "${server_status}"
    fi
    return "${logger_status}"
}

complete_shutdown() {
    local had_server=false
    local status

    trap - EXIT
    trap '' SIGTERM SIGINT SIGQUIT
    if [[ -n "${SERVER_PID}" ]]; then
        had_server=true
    fi
    signal_children
    wait_for_server_and_logger
    status=$?
    stop_setup_process
    cleanup_process_files

    if [[ "${had_server}" == "true" ]]; then
        exit "${status}"
    fi
    signal_exit_status "${SHUTDOWN_SIGNAL}"
    exit $?
}

# Reached through the EXIT trap below.
# shellcheck disable=SC2329
cleanup_on_exit() {
    local original_status=$?

    trap - EXIT
    trap '' SIGTERM SIGINT SIGQUIT
    SHUTDOWN_SIGNAL="TERM"
    signal_children
    wait_for_server_and_logger >/dev/null 2>&1
    stop_setup_process
    cleanup_process_files
    exit "${original_status}"
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
    # shellcheck source=/dev/null
    if source replica_setup.sh; then
        echo "Replica setup completed."
    else
        echo "Error: Replica setup failed."
        exit 1
    fi
}

run_server_in_background() {
    local status

    cd "$DATA_PATH" || { echo "Error: Could not change directory to ${DATA_PATH}"; return 1; }
    rm -f "${LOG_PIPE}"
    status=$?
    if [[ ${status} -ne 0 ]]; then
        return "${status}"
    fi
    mkfifo "${LOG_PIPE}"
    status=$?
    if [[ ${status} -ne 0 ]]; then
        return "${status}"
    fi
    if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
        complete_shutdown
    fi

    exec 9<> "${LOG_PIPE}"
    status=$?
    if [[ ${status} -ne 0 ]]; then
        return "${status}"
    fi
    LOG_PIPE_GUARD_OPEN=true
    if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
        complete_shutdown
    fi

    # FD 9 keeps both FIFO ends open until both child PIDs are known. Each child
    # closes its inherited guard before opening the endpoint it actually owns.
    myduckserver "${SERVER_OPTIONS[@]}" 9>&- > "${LOG_PIPE}" 2>&1 &
    SERVER_PID=$!
    SERVER_SIGNAL_SENT=false
    signal_children
    if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
        complete_shutdown
    fi

    tee -a "${LOG_PATH}/server.log" 9>&- < "${LOG_PIPE}" &
    LOGGER_PID=$!
    signal_children
    close_log_pipe_guard
    if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
        complete_shutdown
    fi

    printf '%s\n' "${SERVER_PID}" > "${PID_TEMP_FILE}"
    status=$?
    if [[ ${status} -eq 0 ]]; then
        mv "${PID_TEMP_FILE}" "${PID_FILE}"
        status=$?
    fi
    if [[ ${status} -ne 0 ]]; then
        return "${status}"
    fi
    if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
        complete_shutdown
    fi
}

wait_for_my_duck_server_ready() {
    local host="127.0.0.1"
    local user="root"
    local port="3306"
    local max_attempts=30
    local attempt=0
    local wait_time=2
    local status

    echo "Waiting for MyDuck Server at $host:$port to be ready..."

    until run_setup_command mysqlsh --sql --host "$host" --port "$port" --user "$user" "${MYSQL_PASSWORD_OPTION}" --execute "SELECT VERSION();" &> /dev/null; do
        if [[ -n "${SERVER_PID}" ]] && ! kill -0 "${SERVER_PID}" 2>/dev/null; then
            wait_for_pid "${SERVER_PID}"
            status=$?
            SERVER_PID=""
            SERVER_SIGNAL_SENT=false
            echo "CRITICAL: MyDuck Server process died during startup."
            if [[ ${status} -eq 0 ]]; then
                return 1
            fi
            return "${status}"
        fi
        if [[ -n "${LOGGER_PID}" ]] && ! kill -0 "${LOGGER_PID}" 2>/dev/null; then
            wait_for_pid "${LOGGER_PID}"
            status=$?
            LOGGER_PID=""
            echo "CRITICAL: server logger process died during startup."
            if [[ ${status} -eq 0 ]]; then
                return 1
            fi
            return "${status}"
        fi
        attempt=$((attempt+1))
        if [ "$attempt" -ge "$max_attempts" ]; then
            echo "Error: MySQL connection timeout after $max_attempts attempts."
            return 1
        fi
        echo "Attempt $attempt/$max_attempts: MyDuck Server is unavailable - retrying in $wait_time seconds..."
        sleep $wait_time
    done

    echo "MyDuck Server is ready!"
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
            run_setup_command mysqlsh --sql --host "$host" --port "$mysql_port" --user "$mysql_user" "${MYSQL_PASSWORD_OPTION}" --file="$file" || true
        done
    fi
    if [ -d "$INIT_SQLS_DIR/postgres" ] && [ "$(find "$INIT_SQLS_DIR/postgres" -maxdepth 1 -name '*.sql' -type f | head -n 1)" ]; then
        echo "Executing init SQL scripts from $INIT_SQLS_DIR/postgres..."
        for file in "$INIT_SQLS_DIR/postgres"/*.sql; do
            echo "Executing $file..."
            run_setup_command env PGPASSWORD="$SUPERUSER_PASSWORD" psql -h "$host" -p "$postgres_port" -U "$postgres_user" -f "$file" || true
        done
    fi
    return 0
}

# Handle the setup_mode
setup() {
    if [ -n "$DEFAULT_DB" ]; then
        SERVER_OPTIONS+=("--default-db=$DEFAULT_DB")
    fi

    if [ -n "$SUPERUSER_PASSWORD" ]; then
        SERVER_OPTIONS+=("--superuser-password=$SUPERUSER_PASSWORD")
    fi

    if [ -n "$LOG_LEVEL" ]; then
        SERVER_OPTIONS+=("--loglevel=$LOG_LEVEL")
    fi
    
    if [ -n "$PROFILER_PORT" ]; then
        SERVER_OPTIONS+=("--profiler-port=$PROFILER_PORT")
    fi

    if [ -n "$RESTORE_FILE" ]; then
        SERVER_OPTIONS+=("--restore-file=$RESTORE_FILE")
    fi

    if [ -n "$RESTORE_ENDPOINT" ]; then
        SERVER_OPTIONS+=("--restore-endpoint=$RESTORE_ENDPOINT")
    fi

    if [ -n "$RESTORE_ACCESS_KEY_ID" ]; then
        SERVER_OPTIONS+=("--restore-access-key-id=$RESTORE_ACCESS_KEY_ID")
    fi

    if [ -n "$RESTORE_SECRET_ACCESS_KEY" ]; then
        SERVER_OPTIONS+=("--restore-secret-access-key=$RESTORE_SECRET_ACCESS_KEY")
    fi

    # Ensure required directories exist
    mkdir -p "${DATA_PATH}" "${LOG_PATH}" || return $?
    if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
        complete_shutdown
    fi

    case "$SETUP_MODE" in
        "" | "SERVER")
            echo "Starting MyDuck Server in SERVER mode..."
            run_server_in_background || return $?
            wait_for_my_duck_server_ready || return $?
            execute_init_sqls
            ;;
        "REPLICA")
            echo "Starting MyDuck Server in REPLICA mode..."
            parse_dsn
            run_server_in_background || return $?
            wait_for_my_duck_server_ready || return $?
            execute_init_sqls
            run_replica_setup
            ;;
        *)
            echo "Error: Invalid SETUP_MODE value. Valid options are: SERVER, REPLICA."
            exit 1
            ;;
    esac
}

trap 'request_shutdown TERM' SIGTERM
trap 'request_shutdown INT' SIGINT
trap 'request_shutdown QUIT' SIGQUIT
trap cleanup_on_exit EXIT

setup
setup_status=$?
if [[ ${setup_status} -ne 0 ]]; then
    exit "${setup_status}"
fi

if [[ -n "${SHUTDOWN_SIGNAL}" ]]; then
    complete_shutdown
fi

wait_for_server_and_logger
server_status=$?
cleanup_process_files
if [[ -z "${SHUTDOWN_SIGNAL}" && ${server_status} -eq 0 ]]; then
    echo "CRITICAL: MyDuck Server process exited unexpectedly."
    exit 1
fi
exit "${server_status}"
