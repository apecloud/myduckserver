import mysql.connector

class MySQLTest:
    class Test:
        def __init__(self, query, expected_results):
            self.query = query
            self.expected_results = expected_results

        def run(self, cursor):
            print(f"Running test: {self.query}")
            cursor.execute(self.query)
            if cursor.description is None:
                if len(self.expected_results) != 0:
                    print(f"Expected {len(self.expected_results)} rows, got 0")
                    return False
                print("Returns 0 rows")
                return True

            rows = cursor.fetchall()
            if len(rows[0]) != len(self.expected_results[0]):
                print(f"Expected {len(self.expected_results[0])} columns, got {len(rows[0])}")
                return False

            for row, expected_row in zip(rows, self.expected_results):
                if list(map(str, row)) != list(map(str, expected_row)):
                    print(f"Expected: {list(map(str, expected_row))}, got: {list(map(str, row))}")
                    return False

            print(f"Returns {len(rows)} rows")
            if len(rows) != len(self.expected_results):
                print(f"Expected {len(self.expected_results)} rows")
                return False

            return True

    def __init__(self):
        self.conn = None
        self.tests = []

    def connect(self, ip, port, user, password):
        try:
            self.conn = mysql.connector.connect(
                host=ip,
                port=port,
                user=user,
                password=password
            )
        except Exception as e:
            raise RuntimeError(e)

    def disconnect(self):
        self.conn.close()

    def add_test(self, query, expected_results):
        self.tests.append(self.Test(query, expected_results))

    def run_tests(self):
        for test in self.tests:
            cursor = None
            try:
                cursor = self.conn.cursor()
                if not test.run(cursor):
                    return False
            except Exception as e:
                print(f"Error running test: {e}")
                return False
            finally:
                if cursor:
                    cursor.close()
        return True

    def read_tests_from_file(self, filename):
        with open(filename, "r") as file:
            lines = file.readlines()

        query = None
        expected_results = []
        for line in lines:
            line = line.strip()
            if not line:
                if query:
                    self.add_test(query, expected_results)
                    query = None
                    expected_results = []
            elif query is None:
                query = line
            else:
                expected_results.append(line.split(","))

        if query:
            self.add_test(query, expected_results)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 6:
        print(f"Usage: {sys.argv[0]} <ip> <port> <user> <password> <testFile>")
        sys.exit(1)

    mysql_test = MySQLTest()
    mysql_test.connect(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4])
    mysql_test.read_tests_from_file(sys.argv[5])

    if not mysql_test.run_tests():
        mysql_test.disconnect()
        sys.exit(1)

    mysql_test.disconnect()