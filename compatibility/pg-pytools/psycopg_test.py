from psycopg import sql
import psycopg

rows = [
    (1, 100, "aaa"),
    (2, 200, "bbb"),
    (3, 300, "ccc"),
    (4, 400, "ddd"),
    (5, 500, "eee"),
]

# Connect to an existing database
with psycopg.connect("dbname=postgres user=postgres host=127.0.0.1 port=5432", autocommit=True) as conn:
    # Open a cursor to perform database operations
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS test CASCADE")
        cur.execute("CREATE SCHEMA test")

        cur.execute("""
            CREATE TABLE test.tb1 (
                id integer PRIMARY KEY,
                num integer,
                data text)
            """)


        # Pass data to fill a query placeholders and let Psycopg perform the correct conversion
        cur.execute(
            "INSERT INTO test.tb1 (id, num, data) VALUES (%s, %s, %s)",
            rows[0])

        # Query the database and obtain data as Python objects
        cur.execute("SELECT * FROM test.tb1")
        row = cur.fetchone()
        assert row == rows[0], "Row is not equal"

        # Copy data from a file-like object to a table
        print("Copy data from a file-like object to a table")
        with cur.copy("COPY test.tb1 (id, num, data) FROM STDIN") as copy:
            for row in rows[1:3]:
                copy.write(f"{row[0]}\t{row[1]}\t{row[2]}\n".encode())
            for row in rows[3:]:
                copy.write_row(row)

        # Copy data from a table to a file-like object
        print("Copy data from a table to a file-like object")
        with cur.copy(
                "COPY (SELECT * FROM test.tb1 LIMIT %s) TO STDOUT",
                (4,)
        ) as copy:
            copy.set_types(["int4", "int4", "text"])
            for i, row in enumerate(copy.rows()):
                assert row == rows[i], f"Row {i} is not equal"
