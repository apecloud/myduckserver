#!/usr/bin/env bats

@test "Ensure every database reported by SHOW DATABASES is unique" {
  run mysql -h 127.0.0.1 -u root --raw --batch --skip-column-names -e "SHOW DATABASES"
  [ "$status" -eq 0 ]
  unique_databases=$(echo "$output" | sort | uniq -d)
  [ -z "$unique_databases" ]
}
