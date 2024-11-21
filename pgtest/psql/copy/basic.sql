CREATE SCHEMA IF NOT EXISTS test_psql_copy_from;

USE test_psql_copy_from;

CREATE TABLE t (a int, b text);

\copy t FROM 'pgtest/testdata/basic.csv' (FORMAT CSV, DELIMITER ',', HEADER);

\copy t FROM 'pgtest/testdata/basic.csv' WITH DELIMITER ',' CSV HEADER;

SELECT * FROM t;