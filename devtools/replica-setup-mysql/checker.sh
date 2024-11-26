#!/bin/bash

# Function to check if a command was successful
check_command() {
    if [[ $? -ne 0 ]]; then
        echo "Error: $1 failed."
        exit 1
    fi
}

# Function to check MySQL server parameters
check_server_params() {
    echo "Checking MySQL server parameters..."

    # Retrieve the required MySQL server variables using mysqlsh
    result=$(mysqlsh --uri="$SOURCE_DSN" --sql -e "
    SHOW VARIABLES WHERE variable_name IN ('binlog_format', 'enforce_gtid_consistency', 'gtid_mode', 'gtid_strict_mode', 'log_bin');
    ")

    check_command "retrieving server parameters"

    # Check if the result is empty or contains errors
    if [[ -z "$result" || "$result" == *"ERROR"* ]]; then
        echo "Error: Could not retrieve server parameters."
        return 1
    fi

    # Check for each parameter and validate their values
    binlog_format=$(echo "$result" | grep -i "binlog_format" | awk '{print $2}')
    enforce_gtid_consistency=$(echo "$result" | grep -i "enforce_gtid_consistency" | awk '{print $2}')
    gtid_mode=$(echo "$result" | grep -i "gtid_mode" | awk '{print $2}' | tr '[:lower:]' '[:upper:]')
    gtid_strict_mode=$(echo "$result" | grep -i "gtid_strict_mode" | awk '{print $2}' | tr '[:lower:]' '[:upper:]')
    log_bin=$(echo "$result" | grep -i "log_bin" | awk '{print $2}')

    # Validate binlog_format
    if [[ "$binlog_format" != "ROW" ]]; then
        echo "Error: binlog_format is not set to 'ROW', it is set to '$binlog_format'."
        return 1
    fi

    # MariaDB use gtid_strict_mode instead of gtid_mode
    if [[ "$gtid_strict_mode" == "OFF" || (-z "$gtid_strict_mode" && "${gtid_mode}" =~ ^OFF) ]]; then
        GTID_MODE="OFF"
        echo "GTID_MODE: $GTID_MODE"
    fi

    # If gtid_strict_mode is empty, check gtid_mode. If it's not OFF, then enforce_gtid_consistency must be ON
    if [[ -z "$gtid_strict_mode" && $GTID_MODE == "ON" && "$enforce_gtid_consistency" != "ON" ]]; then
        echo "Error: gtid_mode is not set to 'OFF', it is set to '$gtid_mode'. enforce_gtid_consistency must be 'ON'."
        return 1
    fi

    # Validate log_bin
    if [[ "$log_bin" != "ON" && "$log_bin" != "1" ]]; then
        echo "Error: log_bin is not enabled. Current value is '$log_bin'."
        return 1
    fi

    echo "MySQL server parameters are correctly configured."
    return 0
}

# Function to check MySQL current user privileges
check_user_privileges() {
    echo "Checking privileges for the current user '$SOURCE_USER'..."

    # Check the user grants for the currently authenticated user using mysqlsh
    result=$(mysqlsh --host="$SOURCE_HOST" --port="$SOURCE_PORT" --user="$SOURCE_USER" --password="$SOURCE_PASSWORD" --sql -e "
    SHOW GRANTS FOR CURRENT_USER();
    ")

    check_command "retrieving user grants"

    # Check if the required privileges are granted or if GRANT ALL is present
    if echo "$result" | grep -q -E "GRANT (SELECT|RELOAD|REPLICATION CLIENT|REPLICATION SLAVE|SHOW VIEW|EVENT)"; then
        echo "Current user '$SOURCE_USER' has all required privileges."
    elif echo "$result" | grep -q "GRANT ALL"; then
        echo "Current user '$SOURCE_USER' has 'GRANT ALL' privileges."
    else
        echo "Error: Current user '$SOURCE_USER' is missing some required privileges."
        return 1
    fi

    return 0
}

# Function to check MySQL configuration
check_mysql_config() {
    check_server_params
    check_command "MySQL server parameters check"

    check_user_privileges
    check_command "User privileges check"

    return 0
}

# Function to check if source MySQL server is empty
check_if_source_mysql_is_empty() {
    # Run the query using mysqlsh and capture the output
    OUTPUT=$(mysqlsh --uri "$SOURCE_DSN" --sql -e "SHOW DATABASES;" 2>/dev/null)

    check_command "retrieving database list"

    # Check if the output contains only the default databases
    NON_DEFAULT_DBs=$(echo "$OUTPUT" | grep -cv -E "^(Database|information_schema|mysql|performance_schema|sys)$")

    if [[ "$NON_DEFAULT_DBs" -gt 0 ]]; then
        return 1
    else
        return 0
    fi
}

# Function to check if there is ongoing replication on MyDuck Server
check_if_myduck_has_replica() {
    REPLICA_STATUS=$(mysqlsh --sql --host=$MYDUCK_HOST --port=$MYDUCK_PORT --user=root --password='' -e "SHOW REPLICA STATUS\G")
    check_command "retrieving replica status"

    SOURCE_HOST_EXISTS=$(echo "$REPLICA_STATUS" | awk '/Source_Host/ {print $2}')

    # Check if Source_Host is not null or empty
    if [[ -n "$SOURCE_HOST_EXISTS" ]]; then
        echo "Replication has already been started. Source Host: $SOURCE_HOST_EXISTS"
        return 1
    else
        return 0
    fi
}

