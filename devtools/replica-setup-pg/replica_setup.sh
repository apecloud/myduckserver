#!/bin/bash

usage() {
    echo "Usage: $0 --pg_dump /path/to/pg_dump [--myduck_db <database>] [--myduck_host <host>] [--myduck_port <port>] [--myduck_user <user>] [--myduck_password <password>] [--myduck_in_docker <true|false>]"
    exit 1
}

MYDUCK_DB=${MYDUCK_DB:-mysql}
MYDUCK_HOST=${MYDUCK_HOST:-127.0.0.1}
MYDUCK_PORT=${MYDUCK_PORT:-5432}
MYDUCK_USER=${MYDUCK_USER:-root}
MYDUCK_PASSWORD=${MYDUCK_PASSWORD:-}
MYDUCK_SERVER_ID=${MYDUCK_SERVER_ID:-2}
MYDUCK_IN_DOCKER=${MYDUCK_IN_DOCKER:-false}

while [[ $# -gt 0 ]]; do
    case $1 in
        --pg_dump)
            PG_DUMP="$2"
            shift 2
            ;;
        --myduck_db)
            MYDUCK_DB="$2"
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
if [[ -z $MYDUCK_DB ]]; then
    echo "Error: Missing required parameter --myduck_db."
    usage
fi

# Step 1: Check if psql exists, if not, install it
if ! command -v psql &> /dev/null; then
    echo "psql not found, attempting to install..."
    bash install_psql.sh
    check_command "psql installation"
else
    echo "psql is already installed."
fi

# Step 2: Check if replication has already been started
#echo "Checking if replication has already been started..."
#check_if_myduck_has_replica
#if [[ $? -ne 0 ]]; then
#    echo "Replication has already been started. Exiting."
#    exit 1
#fi

# Step 3: Prepare MyDuck Server for replication
#echo "Preparing MyDuck Server for replication..."
#source prepare.sh
#check_command "preparing MyDuck Server for replication"

# Step 4: Establish replication
#echo "Starting replication..."
#source start_replication.sh
#check_command "starting replication"

# Step 5: Load the existing data from pg_dump file
if [[ -n "$PG_DUMP" ]]; then
    echo "Loading the snapshot from pg_dump to MyDuck Server..."
    source snapshot.sh
    check_command "loading a snapshot from pg_dump"
else
    echo "No pg_dump file specified. Skipping snapshot."
fi