#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>

typedef struct {
    char *query;
    char **expectedResults;
    int expectedRows;
    int expectedCols;
} Test;

typedef struct {
    PGconn *conn;
    Test *tests;
    int testCount;
} PGTest;

void connectDB(PGTest *pgTest, const char *ip, int port, const char *user, const char *password) {
    char conninfo[256];
    snprintf(conninfo, sizeof(conninfo), "host=%s port=%d dbname=main user=%s password=%s", ip, port, user, password);
    pgTest->conn = PQconnectdb(conninfo);
    if (PQstatus(pgTest->conn) != CONNECTION_OK) {
        printf("Connection to database failed: %s", PQerrorMessage(pgTest->conn));
        PQfinish(pgTest->conn);
        exit(1);
    }
}

void disconnectDB(PGTest *pgTest) {
    PQfinish(pgTest->conn);
}

void addTest(PGTest *pgTest, const char *query, char **expectedResults, int expectedRows, int expectedCols) {
    pgTest->tests = realloc(pgTest->tests, sizeof(Test) * (pgTest->testCount + 1));
    pgTest->tests[pgTest->testCount].query = strdup(query);
    pgTest->tests[pgTest->testCount].expectedResults = expectedResults;
    pgTest->tests[pgTest->testCount].expectedRows = expectedRows;
    pgTest->tests[pgTest->testCount].expectedCols = expectedCols;
    pgTest->testCount++;
}

int runTests(PGTest *pgTest) {
    for (int i = 0; i < pgTest->testCount; i++) {
        Test *test = &pgTest->tests[i];
        printf("Running test: %s\n", test->query);
        PGresult *res = PQexec(pgTest->conn, test->query);
        if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK) {
            printf("Query failed: %s\n", PQerrorMessage(pgTest->conn));
            PQclear(res);
            return 0;
        }
        if (PQresultStatus(res) == PGRES_TUPLES_OK) {
            int rows = PQntuples(res);
            int cols = PQnfields(res);
            if (cols != test->expectedCols) {
                printf("Expected %d columns, got %d\n", test->expectedCols, cols);
                PQclear(res);
                return 0;
            }
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    char *result = PQgetvalue(res, r, c);
                    if (strcmp(result, test->expectedResults[r * cols + c]) != 0) {
                        printf("Expected: '%s', got: '%s'\n", test->expectedResults[r * cols + c], result);
                        PQclear(res);
                        return 0;
                    }
                }
            }
            if (rows != test->expectedRows) {
                printf("Expected %d rows, got %d\n", test->expectedRows, rows);
                PQclear(res);
                return 0;
            }
        }
        PQclear(res);
    }
    return 1;
}

size_t removeNewline(char *line) {
    size_t len = strlen(line);
    if (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
        line[--len] = '\0';
    }
    if (len > 0 && line[len - 1] == '\r') {
        line[--len] = '\0';
    }
    return len;
}

void readTestsFromFile(PGTest *pgTest, const char *filename) {
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
        addTest(pgTest, query, expectedResults, expectedRows, expectedCols);
    }

    fclose(file);
}

int main(int argc, char *argv[]) {
    if (argc != 6) {
        printf("Usage: %s <ip> <port> <user> <password> <testFile>\n", argv[0]);
        return 1;
    }

    PGTest pgTest = {0};
    connectDB(&pgTest, argv[1], atoi(argv[2]), argv[3], argv[4]);

    readTestsFromFile(&pgTest, argv[5]);

    int result = runTests(&pgTest);
    disconnectDB(&pgTest);
    free(pgTest.tests);
    return result ? 0 : 1;
}