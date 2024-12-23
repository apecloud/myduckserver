#!/usr/bin/env bats

load helper

@test "cli_show_all_tables" {
    psql_exec "SHOW ALL TABLES;" | grep -q '__sys__'
}

# `DISCARD ALL` should clear all temp tables
@test "discard_all_clears_temp_tables" {
    # Run all SQL statements in a single session
    run psql_exec_stdin <<-EOF
        CREATE TEMP TABLE tt (id int);
        INSERT INTO tt VALUES (1), (2);
        SELECT COUNT(*) FROM tt;
        DISCARD ALL;
        SELECT COUNT(*) FROM tt;
EOF

    [ "$status" -ne 0 ]
    [[ "${output}" == *"Table with name tt does not exist"* ]]
}
