#!/usr/bin/env bats

setup() {
    psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -c "DROP SCHEMA IF EXISTS test CASCADE;"
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

@test "pg-c" {
    start_process gcc -o $BATS_TEST_DIRNAME/c/pg_test $BATS_TEST_DIRNAME/c/pg_test.c -I/usr/include/postgresql -lpq
    start_process $BATS_TEST_DIRNAME/c/pg_test 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}

# @test "pg-csharp" {
#     set_custom_teardown "sudo pkill -f dotnet"
#     start_process dotnet build $BATS_TEST_DIRNAME/csharp/PGTest.csproj -o $BATS_TEST_DIRNAME/csharp/bin
#     start_process dotnet $BATS_TEST_DIRNAME/csharp/bin/PGTest.dll 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
# }

@test "pg-go" {
    start_process go build -o $BATS_TEST_DIRNAME/go/pg $BATS_TEST_DIRNAME/go/pg.go
    start_process $BATS_TEST_DIRNAME/go/pg 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}

@test "pg-java" {
    start_process javac $BATS_TEST_DIRNAME/java/PGTest.java
    start_process java -cp $BATS_TEST_DIRNAME/java:$BATS_TEST_DIRNAME/java/postgresql-42.7.4.jar PGTest 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}

@test "pg-node" {
    start_process node $BATS_TEST_DIRNAME/node/pg_test.js 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}

@test "pg-perl" {
    start_process perl $BATS_TEST_DIRNAME/perl/pg_test.pl 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}

@test "pg-php" {
    start_process php $BATS_TEST_DIRNAME/php/pg_test.php 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}

@test "pg-python" {
    start_process python3 $BATS_TEST_DIRNAME/python/pg_test.py 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}

@test "pg-r" {
    start_process Rscript $BATS_TEST_DIRNAME/r/PGTest.R 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}

@test "pg-ruby" {
    start_process ruby $BATS_TEST_DIRNAME/ruby/pg_test.rb 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}

@test "pg-rust" {
    start_process cargo build --release --manifest-path $BATS_TEST_DIRNAME/rust/Cargo.toml
    start_process $BATS_TEST_DIRNAME/rust/target/release/pg_test 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
}