#!/usr/bin/env bats

load helper

@test "cli_show_all_tables" {
    psql_exec "SHOW ALL TABLES;" | grep -q '__sys__'
}