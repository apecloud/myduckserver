#!/usr/bin/env bats

load helper

setup_file() {
    mysql_exec_stdin <<-'EOF'
    CREATE DATABASE table_statement_test;
    USE table_statement_test;
    CREATE TABLE peildatum (id INT, name VARCHAR(255));
    INSERT INTO peildatum VALUES (1, 'test1'), (2, 'test2');
EOF
}

teardown_file() {
    mysql_exec_stdin <<-'EOF'
    DROP DATABASE IF EXISTS table_statement_test;
EOF
}

@test "TABLE statement should return all rows from the table" {
    run mysql_exec "TABLE table_statement_test.peildatum"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "1	test1" ]
    [ "${lines[1]}" = "2	test2" ]
}
