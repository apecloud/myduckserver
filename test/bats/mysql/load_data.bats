#!/usr/bin/env bats
bats_require_minimum_version 1.5.0

load helper

setup_file() {
    mysql_exec_stdin <<-'EOF'
    CREATE DATABASE load_data_test;
    SET GLOBAL local_infile = 1;
EOF
}

teardown_file() {
    mysql_exec_stdin <<-'EOF'
    DROP DATABASE IF EXISTS load_data_test;
EOF
}

@test "Load a TSV file that contains an escaped JSON column" {
    skip
    mysql_exec_stdin <<-'EOF'
    USE load_data_test;
    CREATE TABLE translations (code VARCHAR(100), domain VARCHAR(16), translations JSON);
    LOAD DATA LOCAL INFILE 'testdata/issue329.tsv' REPLACE INTO TABLE translations CHARACTER SET 'utf8mb4' FIELDS TERMINATED BY '	' ESCAPED BY '\\' LINES STARTING BY '' TERMINATED BY '\n' (`code`, `domain`, `translations`);
EOF
    run -0 mysql_exec 'SELECT COUNT(*) FROM load_data_test.translations'
    [ "${output}" = "1" ]
}

@test "Load a TSV file with date and enum columns" {
    local tempfile
    tempfile=$(create_temp_file "2025-01-06\t2025-01-06\t2025-01-06\tphprapporten")

    mysql_exec_stdin <<-EOF
    USE load_data_test;
    CREATE TABLE peildatum (
        datum date DEFAULT NULL,
        vanaf date DEFAULT NULL,
        tot date DEFAULT NULL,
        doel enum('phprapporten','excelrapporten','opslagkosten') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    LOAD DATA LOCAL INFILE '${tempfile}' REPLACE INTO TABLE peildatum
    CHARACTER SET 'utf8mb4'
    FIELDS TERMINATED BY '	' ESCAPED BY '\\\\'
    LINES STARTING BY '' TERMINATED BY '\n'
    (datum, vanaf, tot, doel);
EOF
    
    run -0 mysql_exec 'SELECT * FROM load_data_test.peildatum'
    [ "${output}" = "2025-01-06	2025-01-06	2025-01-06	phprapporten" ]
    
    rm "$tempfile"
}