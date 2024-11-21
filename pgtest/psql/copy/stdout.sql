CREATE SCHEMA IF NOT EXISTS test_psql_copy_to;

USE test_psql_copy_to;

CREATE TABLE t (a int, b text, c float);

\o 'stdout.csv'

COPY t TO STDOUT;

\copy t (a, b) TO STDOUT (FORMAT CSV);

COPY t TO STDOUT (FORMAT CSV, HEADER false, DELIMITER '|');

\copy (SELECT a * a, b, c + a FROM t) TO STDOUT (FORMAT CSV, HEADER false, DELIMITER '|');