#!/usr/bin/env bash

# Default MySQL connection parameters
MYSQL_HOST=${MYSQL_HOST:-"127.0.0.1"}
MYSQL_PORT=${MYSQL_PORT:-"3306"}
MYSQL_USER=${MYSQL_USER:-"root"}

mysql_exec() {
    local query="$1"
    shift
    mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" --raw --batch --skip-column-names "$@" -e "$query"
}

mysql_exec_stdin() {
    mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" --raw --batch --skip-column-names "$@"
}