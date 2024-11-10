#!/bin/bash

export DATA_PATH="${HOME}/data"
export LOG_PATH="${HOME}/log"
export REPLICA_SETUP_PATH="${HOME}/replica-setup"
export PID_FILE="${LOG_PATH}/myduck.pid"

# Function to run replica setup
run_replica_setup() {
    if [ -z "$MYSQL_HOST" ] || [ -z "$MYSQL_PORT" ] || [ -z "$MYSQL_USER" ]; then
        echo "Error: Missing required MySQL connection variables for replica setup."
        exit 1
    fi
    echo "Creating replica with MySQL server at $MYSQL_HOST:$MYSQL_PORT..."
    cd "$REPLICA_SETUP_PATH" || { echo "Error: Could not change directory to ${REPLICA_SETUP_PATH}"; exit 1; }

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
      nohup myduckserver >> "${LOG_PATH}"/server.log 2>&1 &
      echo "$!" > "${PID_FILE}"
}

run_server_in_foreground() {
    cd "$DATA_PATH" || { echo "Error: Could not change directory to ${DATA_PATH}"; exit 1; }
    myduckserver
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
    mkdir -p "${DATA_PATH}"
    mkdir -p "${LOG_PATH}"
    case "$SETUP_MODE" in
        "" | "SERVER")
            echo "Starting MyDuck Server in SERVER mode..."
            run_server_in_foreground
            ;;

        "REPLICA")
            echo "Starting MyDuck Server and running replica setup in REPLICA mode..."
            run_server_in_background
            wait_for_my_duck_server_ready
            run_replica_setup
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
