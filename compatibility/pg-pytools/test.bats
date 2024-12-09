#!/usr/bin/env bats

setup() {
    psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP SCHEMA IF EXISTS test CASCADE;"
    touch /tmp/test_pids
}

custom_teardown=""

set_custom_teardown() {
    custom_teardown="$1"
}

teardown() {
    if [ -n "$custom_teardown" ]; then
        eval "$custom_teardown"
        custom_teardown=""
    fi

    while read -r pid; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            wait "$pid" 2>/dev/null
        fi
    done < /tmp/test_pids
    rm /tmp/test_pids
}

start_process() {
    run timeout 2m "$@"
    echo $! >> /tmp/test_pids
    if [ "$status" -ne 0 ]; then
        echo "$output"
        echo "$stderr"
    fi
    [ "$status" -eq 0 ]
}

@test "pg-psycopg" {
    start_process python3 $BATS_TEST_DIRNAME/psycopg_test.py
}

@test "pg-pyarrow" {
    start_process python3 $BATS_TEST_DIRNAME/pyarrow_test.py
}

@test "pg-polars" {
    start_process python3 $BATS_TEST_DIRNAME/polars_test.py
}