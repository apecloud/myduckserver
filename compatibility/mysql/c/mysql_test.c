#include <stdio.h>
#include <stdlib.h>
#include <mysql/mysql.h>
#include <string.h>

typedef struct {
    char *query;
    char **expectedResults;
    int expectedRows;
    int expectedCols;
} Test;

typedef struct {
    MYSQL *conn;
    Test *tests;
    int testCount;
} MySQLTest;

void connectDB(MySQLTest *mysqlTest, const char *ip, int port, const char *user, const char *password);
void disconnectDB(MySQLTest *mysqlTest);
void addTest(MySQLTest *mysqlTest, const char *query, char **expectedResults, int expectedRows, int expectedCols);
int runTests(MySQLTest *mysqlTest);
void readTestsFromFile(MySQLTest *mysqlTest, const char *filename);
size_t removeNewline(char *line);

void connectDB(MySQLTest *mysqlTest, const char *ip, int port, const char *user, const char *password) {
    mysqlTest->conn = mysql_init(NULL);
    if (mysqlTest->conn == NULL) {
        printf("mysql_init() failed\n");
        exit(1);
    }

    if (mysql_real_connect(mysqlTest->conn, ip, user, password, NULL, port, NULL, 0) == NULL) {
        printf("mysql_real_connect() failed\n");
        mysql_close(mysqlTest->conn);
        exit(1);
    }
}

void disconnectDB(MySQLTest *mysqlTest) {
    mysql_close(mysqlTest->conn);
}

void addTest(MySQLTest *mysqlTest, const char *query, char **expectedResults, int expectedRows, int expectedCols) {
    mysqlTest->tests = realloc(mysqlTest->tests, sizeof(Test) * (mysqlTest->testCount + 1));
    mysqlTest->tests[mysqlTest->testCount].query = strdup(query);
    mysqlTest->tests[mysqlTest->testCount].expectedResults = expectedResults;
    mysqlTest->tests[mysqlTest->testCount].expectedRows = expectedRows;
    mysqlTest->tests[mysqlTest->testCount].expectedCols = expectedCols;
    mysqlTest->testCount++;
}

int runTests(MySQLTest *mysqlTest) {
    for (int i = 0; i < mysqlTest->testCount; i++) {
        Test *test = &mysqlTest->tests[i];
        printf("Running test: %s\n", test->query);
        if (mysql_query(mysqlTest->conn, test->query)) {
            printf("Query failed: %s\n", mysql_error(mysqlTest->conn));
            return 0;
        }

        MYSQL_RES *res = mysql_store_result(mysqlTest->conn);
        if (res) {
            int rows = mysql_num_rows(res);
            int cols = mysql_num_fields(res);
            if (cols != test->expectedCols) {
                printf("Expected %d columns, got %d\n", test->expectedCols, cols);
                mysql_free_result(res);
                return 0;
            }
            MYSQL_ROW row;
            int r = 0;
            while ((row = mysql_fetch_row(res))) {
                for (int c = 0; c < cols; c++) {
                    if (strcmp(row[c], test->expectedResults[r * cols + c]) != 0) {
                        printf("Expected: '%s', got: '%s'\n", test->expectedResults[r * cols + c], row[c]);
                        mysql_free_result(res);
                        return 0;
                    }
                }
                r++;
            }
            if (rows != test->expectedRows) {
                printf("Expected %d rows, got %d\n", test->expectedRows, rows);
                mysql_free_result(res);
                return 0;
            }
            mysql_free_result(res);
        }
    }
    return 1;
}

size_t removeNewline(char *line) {
    size_t len = strlen(line);
    while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
        line[--len] = '\0';
    }
    return len;
}

void readTestsFromFile(MySQLTest *mysqlTest, const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Failed to open test data file");
        exit(1);
    }

    char line[1024];
    while (fgets(line, sizeof(line), file)) {
        size_t len = removeNewline(line);
        if (len == 0) continue;

        char *query = strdup(line);
        char **expectedResults = NULL;
        int expectedRows = 0, expectedCols = 0;

        while (fgets(line, sizeof(line), file)) {
            len = removeNewline(line);
            if (len == 0) break;

            char *token = strtok(line, ",");
            int col = 0;
            while (token) {
                expectedResults = realloc(expectedResults, sizeof(char*) * (expectedRows * expectedCols + col + 1));
                expectedResults[expectedRows * expectedCols + col] = strdup(token);
                token = strtok(NULL, ",");
                col++;
            }
            expectedRows++;
            if (expectedCols == 0) expectedCols = col;
        }
        addTest(mysqlTest, query, expectedResults, expectedRows, expectedCols);
    }

    fclose(file);
}

int main(int argc, char *argv[]) {
    if (argc != 6) {
        printf("Usage: %s <ip> <port> <user> <password> <testFile>\n", argv[0]);
        return 1;
    }

    MySQLTest mysqlTest = {0};
    connectDB(&mysqlTest, argv[1], atoi(argv[2]), argv[3], argv[4]);

    readTestsFromFile(&mysqlTest, argv[5]);

    int result = runTests(&mysqlTest);
    disconnectDB(&mysqlTest);
    free(mysqlTest.tests);
    return result ? 0 : 1;
}