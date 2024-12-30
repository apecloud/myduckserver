#!/usr/bin/env bats
bats_require_minimum_version 1.5.0

load helper

@test "Load a TSV file that contains an escaped JSON column" {
    skip
    mysql_exec_stdin <<-'EOF'
    CREATE DATABASE load_json_column;
    USE load_json_column;
    CREATE TABLE translations (code VARCHAR(100), domain VARCHAR(16), translations JSON);
    SET GLOBAL local_infile = 1;
    LOAD DATA LOCAL INFILE 'testdata/issue329.tsv' REPLACE INTO TABLE `load_json_column`.`translations` CHARACTER SET 'utf8mb4' FIELDS TERMINATED BY '	' ESCAPED BY '\\' LINES STARTING BY '' TERMINATED BY '\n' (`code`, `domain`, `translations`);
EOF
    run -0 mysql_exec 'SELECT COUNT(*) FROM `load_json_column`.`translations`'
    [ "${output}" = "1" ]
}