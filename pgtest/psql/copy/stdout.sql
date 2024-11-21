CREATE SCHEMA IF NOT EXISTS test_psql_copy_to;

USE test_psql_copy_to;

CREATE TABLE t (a int, b text, c float);

INSERT INTO t VALUES (1, 'one', 1.1), (2, 'two', 2.2), (3, 'three', 3.3), (4, 'four', 4.4), (5, 'five', 5.5);

\o 'stdout.csv'

COPY t TO STDOUT;

\copy t (a, b) TO STDOUT (FORMAT CSV);

COPY t TO STDOUT (FORMAT CSV, HEADER false, DELIMITER '|');

\copy (SELECT a * a, b, c + a FROM t) TO STDOUT (FORMAT CSV, HEADER false, DELIMITER '|');

\echo `cat stdout.csv`