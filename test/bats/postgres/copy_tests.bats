#!/usr/bin/env bats
bats_require_minimum_version 1.5.0

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
    # Test copy to CSV file
    tmpfile=$(mktemp)
    psql_exec "\copy test_copy.t TO '${tmpfile}' (FORMAT CSV, HEADER false);"
    run -0 cat "${tmpfile}"
    [ "${lines[0]}" = "1,one,1.1" ]
    [ "${lines[1]}" = "2,two,2.2" ]
    [ "${lines[2]}" = "3,three,3.3" ]

    rm "${tmpfile}"

    # Test copy from CSV with headers
    psql_exec_stdin <<-EOF
        USE test_copy;
        CREATE TABLE csv_test (a int, b text);
        \copy csv_test FROM 'pgtest/testdata/basic.csv' (FORMAT CSV, HEADER);
        \copy csv_test FROM 'pgtest/testdata/basic.csv' WITH DELIMITER ',' CSV HEADER;
EOF
    run -0 psql_exec "SELECT count(*) FROM test_copy.csv_test"
    [ "${output}" != "0" ]

    # Test various CSV output formats
    tmpfile=$(mktemp)
    psql_exec_stdin <<-EOF
        USE test_copy;
        \o '${tmpfile}';
        \copy t TO STDOUT;
        \copy t (a, b) TO STDOUT (FORMAT CSV);
        \copy t TO STDOUT (FORMAT CSV, HEADER false, DELIMITER '|');
        \copy (SELECT a * a, b, c + a FROM t) TO STDOUT (FORMAT CSV, HEADER false, DELIMITER '|');
EOF
    [ "$status" -eq 0 ]
    run -0 cat "${tmpfile}"
    [ "${#lines[@]}" -ge 12 ]
    [ "${lines[3]}" = "1,one" ]
    [ "${lines[6]}" = "1|one|1.1" ]
    [ "${lines[9]}" = "1|one|2.1" ]

    rm "${tmpfile}"
}

@test "copy with parquet format" {
    # Test basic COPY TO PARQUET
    tmpfile=$(mktemp).parquet
    run psql_exec_stdin <<-EOF
        USE test_copy;
        \copy t TO '${tmpfile}' (FORMAT PARQUET);
EOF
    [ "$status" -eq 0 ]
    run -0 duckdb_exec "SELECT COUNT(*) FROM '${tmpfile}'"
    [ "${output}" = "3" ]
    rm "${tmpfile}"

    # Test with column selection
    outfile="test_cols.parquet"
    psql_exec_stdin <<-EOF
        USE test_copy;
        \copy t (a, b) TO '${outfile}' (FORMAT PARQUET);
EOF
    run -0 duckdb_exec "SELECT COUNT(*) FROM '${outfile}'"
    [ "${output}" = "3" ]

    # Test with transformed data
    outfile="test_transform.parquet"
    psql_exec "\copy (SELECT a * a, b, c + a FROM test_copy.t) TO '${outfile}' (FORMAT PARQUET);"
    run duckdb_exec "SELECT COUNT(*) FROM '${outfile}'"
    [ "${output}" = "3" ]
}

@test "copy with arrow format" {
    # Test basic COPY TO ARROW
    outfile="test_out.arrow"
    psql_exec_stdin <<-EOF
        USE test_copy;
        \copy t TO '${outfile}' (FORMAT ARROW);
EOF
    run -0 python3 -c "import pyarrow as pa; reader = pa.ipc.open_stream('${outfile}'); print(len(reader.read_all()))"
    [ "${output}" = "3" ]

    # Test with column selection
    outfile="test_cols.arrow"
    psql_exec_stdin <<-EOF
        USE test_copy;
        \copy t (a, b) TO '${outfile}' (FORMAT ARROW);
EOF
    run -0 python3 -c "import pyarrow as pa; reader = pa.ipc.open_stream('${outfile}'); print(len(reader.read_all()))"
    [ "${output}" = "3" ]

    # Test with transformed data
    outfile="test_transform.arrow"
    psql_exec "\copy (SELECT a * a, b, c + a FROM test_copy.t) TO '${outfile}' (FORMAT ARROW);"
    run -0 python3 -c "import pyarrow as pa; reader = pa.ipc.open_stream('${outfile}'); print(len(reader.read_all()))"
    [ "${output}" == "3" ]

    # Test COPY FROM ARROW
    psql_exec_stdin <<-EOF
        USE test_copy;
        CREATE TABLE arrow_test (a int, b text, c float);
        \copy arrow_test FROM '${outfile}' (FORMAT ARROW);
EOF
    run -0 psql_exec "SELECT COUNT(*) FROM test_copy.arrow_test"
    [ "${output}" == "3" ]
}

@test "copy from database" {
    psql_exec_stdin <<-EOF
        USE test_copy;
        CREATE TABLE db_test (a int, b text);
        INSERT INTO db_test VALUES (1, 'a'), (2, 'b'), (3, 'c');
        ATTACH 'test_copy.db' AS tmp;
        COPY FROM DATABASE myduck TO tmp;
        DETACH tmp;
EOF
}

@test "copy error handling" {
    # Test copying from non-existent schema
    run psql_exec "\copy nonexistent_schema.t TO STDOUT;"
    [ "$status" -ne 0 ]

    # Test copying from non-existent table
    run psql_exec "\copy test_copy.nonexistent_table TO STDOUT;"
    [ "$status" -ne 0 ]

    # Test invalid SQL syntax
    run psql_exec "\copy (SELECT FROM t) TO STDOUT;"
    [ "$status" -ne 0 ]

    # Test copying to non-existent schema
    tmpfile=$(mktemp)
    run psql_exec "\copy nonexistent_schema.new_table FROM '${tmpfile}';"
    [ "$status" -ne 0 ]
    rm "${tmpfile}"
}
