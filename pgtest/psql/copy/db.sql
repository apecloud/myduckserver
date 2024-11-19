CREATE SCHEMA IF NOT EXISTS test_copy_db;

USE test_copy_db;

CREATE TABLE t (a int, b text);

INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');

ATTACH 'test_copy_db.db' AS tmp;

COPY FROM DATABASE mysql TO tmp;

DETACH tmp;