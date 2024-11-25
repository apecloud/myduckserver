#!/usr/bin/env bats

setup() {
    psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP SCHEMA IF EXISTS test CASCADE;"
}

@test "pg-c" {
    gcc -o $BATS_TEST_DIRNAME/c/pg_test $BATS_TEST_DIRNAME/c/pg_test.c -I/usr/include/postgresql -lpq
    run $BATS_TEST_DIRNAME/c/pg_test 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
    if [ "$status" -ne 0 ]; then
        echo "$output"
    fi
    [ "$status" -eq 0 ]
}

@test "pg-java" {
    javac $BATS_TEST_DIRNAME/java/PGTest.java
    run java -cp $BATS_TEST_DIRNAME/java:$BATS_TEST_DIRNAME/java/postgresql-42.7.4.jar PGTest 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
    if [ "$status" -ne 0 ]; then
        echo "$output"
        echo "$stderr"
    fi
    [ "$status" -eq 0 ]
}

@test "pg-node" {
    run node $BATS_TEST_DIRNAME/node/pg_test.js 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
    if [ "$status" -ne 0 ]; then
        echo "$output"
        echo "$stderr"
    fi
    [ "$status" -eq 0 ]
}

@test "pg-perl" {
    run perl $BATS_TEST_DIRNAME/perl/pg_test.pl 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
    if [ "$status" -ne 0 ]; then
        echo "$output"
        echo "$stderr"
    fi
    [ "$status" -eq 0 ]
}

@test "pg-php" {
    run php $BATS_TEST_DIRNAME/php/pg_test.php 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
    if [ "$status" -ne 0 ]; then
        echo "$output"
        echo "$stderr"
    fi
    [ "$status" -eq 0 ]
}

@test "pg-python" {
    run python3 $BATS_TEST_DIRNAME/python/pg_test.py 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
    if [ "$status" -ne 0 ]; then
        echo "$output"
        echo "$stderr"
    fi
    [ "$status" -eq 0 ]
}

@test "pg-ruby" {
    run ruby $BATS_TEST_DIRNAME/ruby/pg_test.rb 127.0.0.1 5432 postgres "" $BATS_TEST_DIRNAME/test.data
    if [ "$status" -ne 0 ]; then
        echo "$output"
        echo "$stderr"
    fi
    [ "$status" -eq 0 ]
}
