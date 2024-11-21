#!/bin/bash

export DATA_PATH="${HOME}/data"
export LOG_PATH="${HOME}/log"
export MYSQL_REPLICA_SETUP_PATH="${HOME}/replica-setup-mysql"
export POSTGRES_REPLICA_SETUP_PATH="${HOME}/replica-setup-postgres"
export PID_FILE="${LOG_PATH}/myduck.pid"

parse_dsn() {
    local dsn="$SOURCE_DSN"

    # Initialize variables
    SOURCE_TYPE=""
    SOURCE_USER=""
    SOURCE_PASSWORD=""
    SOURCE_HOST=""
    SOURCE_PORT=""
    SOURCE_DATABASE=""

    # Detect type
    if [[ "$dsn" =~ ^postgresql:// ]]; then
        SOURCE_TYPE="POSTGRES"
        # Strip the prefix
        dsn="${dsn#postgresql://}"
    elif [[ "$dsn" =~ ^mysql:// ]]; then
        SOURCE_TYPE="MYSQL"
        # Strip the prefix
        dsn="${dsn#mysql://}"
    else
        echo "Error: Unsupported DSN format"
        return 1
    fi

    # Extract credentials and host/port/dbname
    if [[ "$dsn" =~ ^([^:@]+)(:([^@]*))?@([^:/]+)(:([0-9]+))?(/(.+))?$ ]]; then
        SOURCE_USER="${BASH_REMATCH[1]}"
        SOURCE_PASSWORD="${BASH_REMATCH[3]}"
        SOURCE_HOST="${BASH_REMATCH[4]}"
        SOURCE_PORT="${BASH_REMATCH[6]}"
        SOURCE_DATABASE="${BASH_REMATCH[8]}"
    else
        echo "Error: Failed to parse DSN"
        return 1
    fi

    # Handle empty SOURCE_DATABASE
    if [[ -z "$SOURCE_DATABASE" ]]; then
        if [[ "$SOURCE_TYPE" == "POSTGRES" ]]; then
            SOURCE_DATABASE="postgres"
        elif [[ "$SOURCE_TYPE" == "MYSQL" ]]; then
            SOURCE_DATABASE="mysql"
        fi
    fi

    # Debugging (Optional: Comment out in production)
    echo "SOURCE_TYPE=$SOURCE_TYPE"
    echo "SOURCE_USER=$SOURCE_USER"
    echo "SOURCE_PASSWORD=$SOURCE_PASSWORD"
    echo "SOURCE_HOST=$SOURCE_HOST"
    echo "SOURCE_PORT=$SOURCE_PORT"
    echo "SOURCE_DATABASE=$SOURCE_DATABASE"
}

if [ -n "$PGSQL_PRIMARY_DSN" ]; then
    export PGSQL_PRIMARY_DSN_ARG="-pg-primary-dsn $PGSQL_PRIMARY_DSN"
fi

if [ -n "$PGSQL_SLOT_NAME" ]; then
    export PGSQL_SLOT_NAME_ARG="-pg-slot-name $PGSQL_SLOT_NAME"
fi

if [ -n "$LOG_LEVEL" ]; then
    export LOG_LEVEL="-loglevel $LOG_LEVEL"
fi

# Function to run replica setup
run_replica_setup() {
    case "$SOURCE_TYPE" in
        MYSQL)
            echo "Creating replica with MySQL server at $SOURCE_DSN..."
            cd "$MYSQL_REPLICA_SETUP_PATH" || {
                echo "Error: Could not change directory to ${MYSQL_REPLICA_SETUP_PATH}";
                exit 1;
            }
            ;;
        POSTGRES)
            echo "Creating replica with Postgres server at $SOURCE_DSN..."
            cd "$POSTGRES_REPLICA_SETUP_PATH" || {
                echo "Error: Could not change directory to ${POSTGRES_REPLICA_SETUP_PATH}";
                exit 1;
            }
            ;;
        *)
            echo "Error: Invalid SOURCE_TYPE value. Valid options are: MYSQL, POSTGRES."
            exit 1
            ;;
    esac

    # Run replica_setup.sh and check for errors
    if bash replica_setup.sh; then
        echo "Replica setup completed."
    else
        echo "Error: Replica setup failed."
        exit 1
    fi
}

run_server_in_background() {
      cd "$DATA_PATH" || { echo "Error: Could not change directory to ${DATA_PATH}"; exit 1; }
      nohup myduckserver $PGSQL_PRIMARY_DSN_ARG $PGSQL_SLOT_NAME_ARG $LOG_LEVEL >> "${LOG_PATH}"/server.log 2>&1 &
      echo "$!" > "${PID_FILE}"
}

run_server_in_foreground() {
    cd "$DATA_PATH" || { echo "Error: Could not change directory to ${DATA_PATH}"; exit 1; }
    myduckserver $PGSQL_PRIMARY_DSN_ARG $PGSQL_SLOT_NAME_ARG $LOG_LEVEL
}

wait_for_my_duck_server_ready() {
    local host="127.0.0.1"
    local user="root"
    local port="3306"
    local max_attempts=30
    local attempt=0
    local wait_time=2

    echo "Waiting for MyDuck Server at $host:$port to be ready..."

    until mysqlsh --sql --host "$host" --port "$port" --user "$user" --no-password --execute "SELECT VERSION();" &> /dev/null; do
        attempt=$((attempt+1))
        if [ "$attempt" -ge "$max_attempts" ]; then
            echo "Error: MySQL connection timed out after $max_attempts attempts."
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

# Handle the setup_mode
setup() {
    # Ensure required directories exist
    mkdir -p "${DATA_PATH}" "${LOG_PATH}"

    case "$SETUP_MODE" in
        "" | "SERVER")
            echo "Starting MyDuck Server in SERVER mode..."
            run_server_in_foreground
            ;;

        "REPLICA")
            echo "Starting MyDuck Server and running replica setup in REPLICA mode..."

            case "$SOURCE_TYPE" in
                MYSQL)
                    run_server_in_background
                    wait_for_my_duck_server_ready
                    run_replica_setup
                    ;;
                POSTGRES)
                    run_replica_setup
                    run_server_in_background
                    wait_for_my_duck_server_ready
                    ;;
                *)
                    echo "Error: Invalid SOURCE_TYPE value. Valid options are: MYSQL, POSTGRES."
                    exit 1
                    ;;
            esac
            ;;

        *)
            echo "Error: Invalid SETUP_MODE value. Valid options are: SERVER, REPLICA."
            exit 1
            ;;
    esac
}

setup

while [[ "$SETUP_MODE" == "REPLICA" ]]; do
    # Check if the processes have started
    check_process_alive "$PID_FILE" "MyDuck Server"
    MY_DUCK_SERVER_STATUS=$?
    if (( MY_DUCK_SERVER_STATUS != 0 )); then
        echo "MyDuck Server is not running. Exiting..."
        exit 1
    fi

    # Sleep before the next status check
    sleep 10
done
