#!/usr/bin/env bash

# Default Postgres connection parameters
PG_HOST=${PG_HOST:-"127.0.0.1"}
PG_PORT=${PG_PORT:-"5432"}
PG_USER=${PG_USER:-"postgres"}

psql_exec() {
    local query="$1"
    shift
    psql -h "$PG_HOST" -U "$PG_USER" -F ',' --no-align --field-separator ',' -t --pset footer=off -v "ON_ERROR_STOP=1" "$@" -c "$query"
}

psql_exec_stdin() {
    psql -h "$PG_HOST" -U "$PG_USER" -F ',' --no-align --field-separator ',' -t --pset footer=off -v "ON_ERROR_STOP=1" "$@"
}

duckdb_exec() {
    local query="$1"
    shift
    duckdb --csv --noheader "$@" -c "$query"
}