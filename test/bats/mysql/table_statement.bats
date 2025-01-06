#!/usr/bin/env bats
bats_require_minimum_version 1.5.0

load helper

setup_file() {
    mysql_exec_stdin <<-'EOF'
    CREATE DATABASE table_statement_test;
    USE table_statement_test;
    CREATE TABLE t (id INT, name VARCHAR(255));
    INSERT INTO t VALUES (1, 'test1'), (2, 'test2');
EOF
}

teardown_file() {
    mysql_exec_stdin <<-'EOF'
    DROP DATABASE IF EXISTS table_statement_test;
EOF
}

@test "TABLE statement should return all rows from the table" {
    run -0 mysql_exec_stdin <<-'EOF'
    USE table_statement_test;
    TABLE t;
EOF
    [ "${lines[0]}" = "1	test1" ]
    [ "${lines[1]}" = "2	test2" ]
}
