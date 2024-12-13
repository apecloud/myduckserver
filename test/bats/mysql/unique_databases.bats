#!/usr/bin/env bats

load helper

@test "Ensure every database reported by SHOW DATABASES is unique" {
  run mysql_exec "SHOW DATABASES"
  [ "$status" -eq 0 ]
  unique_databases=$(echo "$output" | sort | uniq -d)
  [ -z "$unique_databases" ]
}
