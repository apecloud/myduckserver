##!/usr/bin/env bats

@test "cli_show_databases" {
    mysql -h 127.0.0.1 -P 3306 -u root -e "SHOW DATABASES;" | grep -q 'mysql'
}