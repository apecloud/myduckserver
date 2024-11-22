CREATE SCHEMA IF NOT EXISTS test_psql_copy_to_arrow;

USE test_psql_copy_to_arrow;

CREATE TABLE t (a int, b text, c float);

INSERT INTO t VALUES (1, 'one', 1.1), (2, 'two', 2.2), (3, 'three', 3.3), (4, 'four', 4.4), (5, 'five', 5.5);

\o 'stdout.arrow'

COPY t TO STDOUT (FORMAT ARROW);

\o

\echo `python -c "import pyarrow as pa; reader = pa.ipc.open_stream('stdout.arrow'); print(reader.read_all().to_pandas())"`

\copy t FROM 'stdout.arrow' (FORMAT ARROW);

SELECT * FROM t;