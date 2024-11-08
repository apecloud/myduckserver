#!/bin/bash

# Function to display usage
usage() {
    echo "Usage: $0 --mysql_host <host> --mysql_port <port> --mysql_user <user> --mysql_password <password> [--myduck_host <host>] [--myduck_port <port>] [--myduck_user <user>] [--myduck_password <password>] [--myduck_in_docker <true|false>]"
    exit 1
}

MYDUCK_HOST="127.0.0.1"
MYDUCK_PORT=3306
MYDUCK_USER="_replicator_"
MYDUCK_PASSWORD="_replicator_"
MYDUCK_IN_DOCKER="false"

while [[ $# -gt 0 ]]; do
    case $1 in
        --mysql_host)
            MYSQL_HOST="$2"
            shift 2
            ;;
        --mysql_port)
            MYSQL_PORT="$2"
            shift 2
            ;;
        --mysql_user)
            MYSQL_USER="$2"
            shift 2
            ;;
        --mysql_password)
            MYSQL_PASSWORD="$2"
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
if [[ -z "$MYSQL_HOST" || -z "$MYSQL_PORT" || -z "$MYSQL_USER" ]]; then
    echo "Error: All parameters are required."
    usage
fi

# Step 1: Check if mysqlsh exists, if not, install it
if ! command -v mysqlsh &> /dev/null; then
    echo "mysqlsh not found, attempting to install..."
    bash install_mysql_shell.sh
    check_command "mysqlsh installation"
else
    echo "mysqlsh is already installed."
fi

# Step 2: Check if Replica of MyDuck Server has already been started
echo "Checking if replica of MyDuck Server has already been started..."
check_if_myduck_has_replica
if [[ $? -ne 0 ]]; then
    echo "Replica has already been started. Exiting."
    exit 1
fi

# Step 3: Check MySQL server and user configuration
echo "Checking MySQL server and user configuration..."
check_mysql_config
check_command "MySQL server and user configuration check"

# Step 4: Check if source MySQL server is empty
echo "Checking if source MySQL server is empty..."
check_if_source_mysql_is_empty
SOURCE_IS_EMPTY=$?

# Step 5: Call start_snapshot.sh with MySQL parameters if MySQL server is not empty
if [[ $SOURCE_IS_EMPTY -ne 0 ]]; then
    echo "Starting snapshot..."
    source start_snapshot.sh
    check_command "starting snapshot"
else
    echo "Source MySQL server is empty. Skipping snapshot."
fi

# Step 6: Call start_delta.sh with MySQL parameters
echo "Starting delta..."
source start_delta.sh
check_command "starting delta"
