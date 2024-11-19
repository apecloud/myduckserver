#!/bin/bash

usage() {
    echo "Usage: $0 --postgres_host <host> --postgres_port <port> --postgres_user <user> --postgres_password <password> [--myduck_host <host>] [--myduck_port <port>] [--myduck_user <user>] [--myduck_password <password>] [--myduck_in_docker <true|false>]"
    exit 1
}

MYDUCK_HOST=${MYDUCK_HOST:-127.0.0.1}
MYDUCK_PORT=${MYDUCK_PORT:-3306}
MYDUCK_USER=${MYDUCK_USER:-root}
MYDUCK_PASSWORD=${MYDUCK_PASSWORD:-}
MYDUCK_SERVER_ID=${MYDUCK_SERVER_ID:-2}
MYDUCK_IN_DOCKER=${MYDUCK_IN_DOCKER:-false}
GTID_MODE="ON"

while [[ $# -gt 0 ]]; do
    case $1 in
        --postgres_host)
            SOURCE_HOST="$2"
            shift 2
            ;;
        --postgres_port)
            SOURCE_PORT="$2"
            shift 2
            ;;
        --postgres_user)
            SOURCE_USER="$2"
            shift 2
            ;;
        --postgres_password)
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
        --myduck_in_docker)
            MYDUCK_IN_DOCKER="$2"
            shift 2
            ;;
        *)
            echo "Unknown parameter: $1"
            usage
            ;;
    esac
done

source checker.sh

# Check if all parameters are set
if [[ -z "$SOURCE_HOST" || -z "$SOURCE_PORT" || -z "$SOURCE_USER" ]]; then
    echo "Error: Missing required Postgres connection variables: SOURCE_HOST, SOURCE_PORT, SOURCE_USER."
    usage
fi

# Step 3: Check Postgres configuration
echo "Checking Postgres configuration..."
# TODO(neo.zty): add check for Postgres configuration
check_command "Postgres configuration check"

# Step 3: Prepare MyDuck Server for replication
echo "Preparing MyDuck Server for replication..."
# TODO(neo.zty): add prepare for MyDuck Server for replication
check_command "preparing MyDuck Server for replication"

# Step 5: Copy the existing data if the Postgres instance is not empty
if [[ $SOURCE_IS_EMPTY -ne 0 ]]; then
    echo "Copying a snapshot of the Postgres instance to MyDuck Server..."
    source snapshot.sh
    check_command "copying a snapshot of the Postgres instance"
else
    echo "This Postgres instance is empty. Skipping snapshot."
fi

# Step 6: Establish replication
echo "Starting replication..."
source start_replication.sh
check_command "starting replication"