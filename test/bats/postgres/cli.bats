#!/usr/bin/env bats

load helper

@test "cli_show_all_tables" {
    psql_exec "SHOW ALL TABLES;" | grep -q '__sys__'
}

# `DISCARD ALL` should clear all temp tables
@test "discard_all_clears_temp_tables" {
    # Create temp table and insert data
    psql_exec "CREATE TEMP TABLE tt (id int);"
    psql_exec "INSERT INTO tt VALUES (1), (2);"
    
    # Verify data exists
    run psql_exec "SELECT COUNT(*) FROM tt;"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "2" ]

    # Run DISCARD ALL
    psql_exec "DISCARD ALL;"

    # Verify temp table no longer exists
    run psql_exec "SELECT COUNT(*) FROM tt;"
    [ "$status" -eq 1 ]
    [[ "${output}" == *"Table with name tt does not exist"* ]]
}
