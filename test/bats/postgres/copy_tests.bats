#!/usr/bin/env bats

load helper

setup() {
    psql_exec_stdin <<-EOF
        CREATE SCHEMA IF NOT EXISTS test_copy;
        USE test_copy;
        CREATE OR REPLACE TABLE t (a int, b text, c float);
        INSERT INTO t VALUES (1, 'one', 1.1), (2, 'two', 2.2), (3, 'three', 3.3);
EOF
}

teardown() {
    psql_exec "DROP SCHEMA IF EXISTS test_copy CASCADE;"
    rm -f test_*.{csv,parquet,arrow,db} 2>/dev/null
}

@test "copy with csv format" {
    psql_exec "DROP TABLE IF EXISTS test_copy.csv_test;"
    # Test copy to CSV file
    tmpfile=$(mktemp)
    run psql_exec "\copy test_copy.t TO '${tmpfile}' (FORMAT CSV, HEADER false);"
    [ "$status" -eq 0 ]
    run cat "${tmpfile}"
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" == "1,one,1.1" ]]
    rm "${tmpfile}"

    # Test copy from CSV with headers
    run psql_exec_stdin <<-EOF
        USE test_copy;
        CREATE TABLE csv_test (a int, b text);
        \copy csv_test FROM 'pgtest/testdata/basic.csv' (FORMAT CSV, HEADER);
        \copy csv_test FROM 'pgtest/testdata/basic.csv' WITH DELIMITER ',' CSV HEADER;
        SELECT COUNT(*) FROM csv_test;
EOF
    [ "$status" -eq 0 ]
    [[ "${output}" != "0" ]]

    # Test various CSV output formats
    run psql_exec_stdin <<-EOF
        USE test_copy;
        \copy t TO STDOUT;
        \copy t (a, b) TO STDOUT (FORMAT CSV);
        \copy t TO STDOUT (FORMAT CSV, HEADER false, DELIMITER '|');
        \copy (SELECT a * a, b, c + a FROM t) TO STDOUT (FORMAT CSV, HEADER false, DELIMITER '|');
EOF
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -ge 12 ]
    [[ "${lines[0]}" == "1,one,1.1" ]]
    [[ "${lines[3]}" == "1,one" ]]
    [[ "${lines[6]}" == "1|one|1.1" ]]
    [[ "${lines[9]}" == "1|one|2.1" ]]
}

@test "copy with parquet format" {
    # Test basic COPY TO PARQUET
    tmpfile=$(mktemp).parquet
    run psql_exec_stdin <<-EOF
        USE test_copy;
        \copy t TO '${tmpfile}' (FORMAT PARQUET);
EOF
    [ "$status" -eq 0 ]
    run duckdb -c "SELECT COUNT(*) FROM '${tmpfile}'"
    [ "$status" -eq 0 ]
    [[ "${output}" == "3" ]]
    rm "${tmpfile}"

    # Test with column selection
    outfile="test_cols.parquet"
    run psql_exec_stdin <<-EOF
        USE test_copy;
        \copy t (a, b) TO '${outfile}' (FORMAT PARQUET);
EOF
    [ "$status" -eq 0 ]
    run duckdb -c "SELECT COUNT(*) FROM '${outfile}'"
    [ "$status" -eq 0 ]
    [[ "${output}" == "3" ]]

    # Test with transformed data
    outfile="test_transform.parquet"
    run psql_exec "(SELECT a * a, b, c + a FROM test_copy.t) TO '${outfile}' (FORMAT PARQUET);"
    [ "$status" -eq 0 ]
    run duckdb -c "SELECT COUNT(*) FROM '${outfile}'"
    [ "$status" -eq 0 ]
    [[ "${output}" == "3" ]]
}

@test "copy with arrow format" {
    psql_exec "DROP TABLE IF EXISTS test_copy.arrow_test;"
    # Test basic COPY TO ARROW
    outfile="test_out.arrow"
    run psql_exec_stdin <<-EOF
        USE test_copy;
        \copy t TO '${outfile}' (FORMAT ARROW);
EOF
    [ "$status" -eq 0 ]
    run python3 -c "import pyarrow as pa; reader = pa.ipc.open_stream('${outfile}'); print(len(reader.read_all()))"
    [ "$status" -eq 0 ]
    [[ "${output}" == "3" ]]

    # Test with column selection
    outfile="test_cols.arrow"
    run psql_exec_stdin <<-EOF
        USE test_copy;
        \copy t (a, b) TO '${outfile}' (FORMAT ARROW);
EOF
    [ "$status" -eq 0 ]
    run python3 -c "import pyarrow as pa; reader = pa.ipc.open_stream('${outfile}'); print(len(reader.read_all()))"
    [ "$status" -eq 0 ]
    [[ "${output}" == "3" ]]

    # Test with transformed data
    outfile="test_transform.arrow"
    run psql_exec "\copy (SELECT a * a, b, c + a FROM test_copy.t) TO '${outfile}' (FORMAT ARROW);"
    [ "$status" -eq 0 ]
    run python3 -c "import pyarrow as pa; reader = pa.ipc.open_stream('${outfile}'); print(len(reader.read_all()))"
    [ "$status" -eq 0 ]
    [[ "${output}" == "3" ]]

    # Test COPY FROM ARROW
    run psql_exec_stdin <<-EOF
        USE test_copy;
        CREATE TABLE arrow_test (a int, b text, c float);
        \copy arrow_test FROM '${outfile}' (FORMAT ARROW);
        SELECT COUNT(*) FROM arrow_test;
EOF
    [ "$status" -eq 0 ]
    [[ "${output}" == "3" ]]
}

@test "copy from database" {
    psql_exec "DROP TABLE IF EXISTS test_copy.db_test;"
    run psql_exec_stdin <<-EOF
        USE test_copy;
        CREATE TABLE db_test (a int, b text);
        INSERT INTO db_test VALUES (1, 'a'), (2, 'b'), (3, 'c');
        ATTACH 'test_copy.db' AS tmp;
        COPY FROM DATABASE mysql TO tmp;
        DETACH tmp;
EOF
    [ "$status" -eq 0 ]
}

@test "copy error handling" {
    # Test copying from non-existent schema
    run psql_exec "\copy nonexistent_schema.t TO STDOUT;"
    [ "$status" -ne 0 ]

    # Test copying from non-existent table
    run psql_exec "\copy test_copy.nonexistent_table TO STDOUT;"
    [ "$status" -ne 0 ]

    # Test copying to non-existent schema
    tmpfile=$(mktemp)
    run psql_exec "\copy nonexistent_schema.new_table FROM '${tmpfile}';"
    [ "$status" -ne 0 ]
    rm "${tmpfile}"

    # Test invalid SQL syntax
    run psql_exec "\copy (SELECT FROM t) TO STDOUT;"
    [ "$status" -ne 0 ]
}
