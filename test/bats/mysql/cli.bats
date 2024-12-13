#!/usr/bin/env bats

load helper

@test "cli_show_databases" {
    mysql_exec "SHOW DATABASES;" | grep -q 'mysql'
}