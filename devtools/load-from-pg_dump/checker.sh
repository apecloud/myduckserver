#!/bin/bash

# Function to check if a command was successful
check_command() {
    if [[ $? -ne 0 ]]; then
        echo "Error: $1 failed."
        exit 1
    fi
}

# Function to check if there is ongoing replication on MyDuck Server
check_if_myduck_has_replica() {
    REPLICA_STATUS=$(mysqlsh --sql --host=$MYDUCK_HOST --port=$MYDUCK_PORT --user=root --password='' -e "SHOW REPLICA STATUS\G")
    check_command "retrieving replica status"

    SOURCE_HOST=$(echo "$REPLICA_STATUS" | awk '/Source_Host/ {print $2}')

    # Check if Source_Host is not null or empty
    if [[ -n "$SOURCE_HOST" ]]; then
        echo "Replication has already been started. Source Host: $SOURCE_HOST"
        return 1
    else
        return 0
    fi
}

