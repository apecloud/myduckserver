CREATE SCHEMA IF NOT EXISTS test_psql_copy_to_parquet;

USE test_psql_copy_to_parquet;

CREATE TABLE t (a int, b text, c float);

INSERT INTO t VALUES (1, 'one', 1.1), (2, 'two', 2.2), (3, 'three', 3.3), (4, 'four', 4.4), (5, 'five', 5.5);

\o 'stdout-1.parquet'

COPY t TO STDOUT (FORMAT PARQUET);

\o 'stdout-2.parquet'

\copy t (a, b) TO STDOUT (FORMAT PARQUET);

\o 'stdout-3.parquet'

COPY t TO STDOUT (FORMAT PARQUET);

\o 'stdout-4.parquet'

\copy (SELECT a * a, b, c + a FROM t) TO STDOUT (FORMAT PARQUET);

\echo `duckdb -c "SELECT * FROM 'stdout-1.parquet'"`

\echo `duckdb -c "SELECT * FROM 'stdout-2.parquet'"`

\echo `duckdb -c "SELECT * FROM 'stdout-3.parquet'"`

\echo `duckdb -c "SELECT * FROM 'stdout-4.parquet'"`