#!/usr/bin/env bats

setup() {
    mysql -h 127.0.0.1 -P 3306 -u root -e "DROP DATABASE IF EXISTS test;"
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
    check_status
}

check_status() {
    if [ "$status" -ne 0 ]; then
        echo "$output"
        echo "$stderr"
    fi
    [ "$status" -eq 0 ]
}

@test "mysql-c" {
    start_process gcc -o $BATS_TEST_DIRNAME/c/mysql_test $BATS_TEST_DIRNAME/c/mysql_test.c -I/usr/include/mysql -lmysqlclient
    start_process $BATS_TEST_DIRNAME/c/mysql_test 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}

@test "mysql-csharp" {
    set_custom_teardown "sudo pkill -f dotnet"
    start_process dotnet build $BATS_TEST_DIRNAME/csharp/MySQLTest.csproj -o $BATS_TEST_DIRNAME/csharp/bin
    start_process dotnet $BATS_TEST_DIRNAME/csharp/bin/MySQLTest.dll 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}

@test "mysql-go" {
    start_process go build -o $BATS_TEST_DIRNAME/go/mysql $BATS_TEST_DIRNAME/go/mysql.go
    start_process $BATS_TEST_DIRNAME/go/mysql 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}

@test "mysql-java" {
    start_process javac $BATS_TEST_DIRNAME/java/MySQLTest.java
    start_process java -cp $BATS_TEST_DIRNAME/java:$BATS_TEST_DIRNAME/java/mysql-connector-java-8.0.30.jar MySQLTest 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}

@test "mysql-node" {
    start_process node $BATS_TEST_DIRNAME/node/mysql_test.js 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}

@test "mysql-perl" {
    start_process perl $BATS_TEST_DIRNAME/perl/mysql_test.pl 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}

@test "mysql-php" {
    start_process php $BATS_TEST_DIRNAME/php/mysql_test.php 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}

@test "mysql-python" {
    start_process python3 $BATS_TEST_DIRNAME/python/mysql_test.py 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}

# @test "mysql-r" {
#     start_process Rscript $BATS_TEST_DIRNAME/r/MySQLTest.R 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
# }

@test "mysql_ruby" {
    start_process ruby $BATS_TEST_DIRNAME/ruby/mysql_test.rb 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}

@test "mysql-rust" {
    start_process cargo build --release --manifest-path $BATS_TEST_DIRNAME/rust/Cargo.toml
    start_process $BATS_TEST_DIRNAME/rust/target/release/mysql_test 127.0.0.1 3306 root "" $BATS_TEST_DIRNAME/test.data
}