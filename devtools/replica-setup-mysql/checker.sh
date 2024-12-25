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
    result=$(mysqlsh --uri="$SOURCE_DSN" $SOURCE_PASSWORD_OPTION --sql -e "
    SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format', 'gtid_mode', 'enforce_gtid_consistency', 'gtid_strict_mode', 'gtid_current_pos', 'gtid_executed');
    ")

    check_command "retrieving server parameters"

    # Check if the result is empty or contains errors
    if [[ -z "$result" || "$result" == *"ERROR"* ]]; then
        echo "Error: Could not retrieve server parameters."
        return 1
    fi

    # Check for each parameter and validate their values
    log_bin=$(echo "$result" | grep -i "log_bin" | awk '{print $2}')
    binlog_format=$(echo "$result" | grep -i "binlog_format" | awk '{print $2}')
    gtid_mode=$(echo "$result" | grep -i "gtid_mode" | awk '{print $2}' | tr '[:lower:]' '[:upper:]')
    enforce_gtid_consistency=$(echo "$result" | grep -i "enforce_gtid_consistency" | awk '{print $2}')
    gtid_strict_mode=$(echo "$result" | grep -i "gtid_strict_mode" | awk '{print $2}' | tr '[:lower:]' '[:upper:]')
    gtid_current_pos=$(echo "$result" | grep -i "gtid_current_pos" | awk '{print $2}')
    gtid_executed=$(echo "$result" | grep -i "gtid_executed" | awk '{print $2}')

    # Validate log_bin
    if [[ "$log_bin" != "ON" && "$log_bin" != "1" ]]; then
        echo "Error: log_bin is not enabled. Current value is '$log_bin'."
        return 1
    fi

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

    # Set GTID_EXECUTED to either gtid_current_pos or gtid_executed
    if [[ -n "$gtid_strict_mode" ]]; then
        SOURCE_IS_MARIADB="true"
        GTID_EXECUTED="$gtid_current_pos"
    else
        GTID_EXECUTED="$gtid_executed"
    fi

    echo "MySQL server parameters are correctly configured."
    return 0
}

# Function to check MySQL current user privileges
check_user_privileges() {
    echo "Checking privileges for the current user '$SOURCE_USER'..."

    # Check the user grants for the currently authenticated user using mysqlsh
    result=$(mysqlsh --uri "$SOURCE_DSN" $SOURCE_PASSWORD_OPTION --sql -e "
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

# Function to check if the source server could be copied using MySQL Shell
check_if_source_supports_copying_instance() {
    # Retrieve the MySQL version using mysqlsh
    result=$(mysqlsh --uri="$SOURCE_DSN" $SOURCE_PASSWORD_OPTION --sql -e "SELECT @@global.version_comment")
    check_command "retrieving MySQL version comment"

    # Currently, Dolt does not support MySQL Shell's copy-instance utility.
    # Check if the MySQL version string contains "Dolt"
    if echo "$result" | grep -q "Dolt"; then
        echo "MySQL Shell's copy-instance utility is not supported by Dolt yet."
        return 1
    fi

    return 0
}

# Function to check if there is ongoing replication on MyDuck Server
check_if_myduck_has_replica() {
    REPLICA_STATUS=$(mysqlsh --sql --host=$MYDUCK_HOST --port=$MYDUCK_PORT --user=${MYDUCK_USER} ${MYDUCK_PASSWORD_OPTION} -e "SHOW REPLICA STATUS\G")
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

