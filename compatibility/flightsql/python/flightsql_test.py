import unittest
from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect

class TestFlightSQLDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Runs once before any tests are executed, used to set up the database connection."""
        headers = {"foo": "bar"}
        cls.conn = connect(
            "grpc://localhost:47470",  # FlightSQL server address
            db_kwargs={
                DatabaseOptions.TLS_SKIP_VERIFY.value: "true",  # Skip TLS verification
                **{f"{DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}{k}": v for k, v in headers.items()}
            }
        )

    @classmethod
    def tearDownClass(cls):
        """Runs once after all tests have been executed, used to close the database connection."""
        cls.conn.close()

    def setUp(self):
        """Runs before each individual test to ensure a clean environment by resetting the database."""
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS intTable")  # Drop the table if it exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS intTable (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50),
                    value INT
                )
            """)  # Create the table
            self.conn.commit()

    def test_insert_and_select(self):
        """Test inserting data and selecting it back to verify correctness."""
        with self.conn.cursor() as cursor:
            # Insert sample data
            cursor.execute("INSERT INTO intTable (id, name, value) VALUES (1, 'TestName', 100)")
            cursor.execute("INSERT INTO intTable (id, name, value) VALUES (2, 'AnotherName', 200)")

            # Select data from the table
            cursor.execute("SELECT * FROM intTable")
            rows = cursor.fetchall()

            # Expected result after insertions
            expected_rows = [(1, 'TestName', 100), (2, 'AnotherName', 200)]
            self.assertEqual(rows, expected_rows, f"Expected rows: {expected_rows}, but got: {rows}")

    def test_drop_table(self):
        """Test dropping the table to ensure the table can be deleted successfully."""
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS intTable")  # Drop the table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='intTable'")  # Check if the table exists
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 0, "Table 'intTable' should be dropped and not exist in the database.")

if __name__ == "__main__":
    unittest.main()