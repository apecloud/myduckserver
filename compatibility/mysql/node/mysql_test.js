const mysql = require('mysql');
const fs = require('fs');

class Test {
    constructor(query, expectedResults) {
        this.query = query;
        this.expectedResults = expectedResults;
    }

    async run(connection) {
        return new Promise((resolve, reject) => {
            console.log(`Running test: ${this.query}`);
            connection.query(this.query, (error, results, fields) => {
                if (error) {
                    console.error(error.message);
                    return resolve(false);
                }
                if (results.length === undefined || results.length === 0) {
                    if (this.expectedResults.length !== 0) {
                        console.error(`Expected ${this.expectedResults.length} rows, got 0`);
                        return resolve(false);
                    }
                    console.log("Returns 0 rows");
                    return resolve(true);
                }
                if (fields.length !== this.expectedResults[0].length) {
                    console.error(`Expected ${this.expectedResults[0].length} columns, got ${fields.length}`);
                    return resolve(false);
                }
                for (let i = 0; i < results.length; i++) {
                    const row = results[i];
                    const expectedRow = this.expectedResults[i];
                    for (let j = 0; j < expectedRow.length; j++) {
                        const columnName = fields[j].name;
                        if (String(row[columnName]) !== String(expectedRow[j])) {
                            console.error(`Expected: ${String(expectedRow[j])}, got: ${String(row[columnName])}`);
                            return resolve(false);
                        }
                    }
                }
                console.log(`Returns ${results.length} rows`);
                if (results.length !== this.expectedResults.length) {
                    console.error(`Expected ${this.expectedResults.length} rows`);
                    return resolve(false);
                }
                resolve(true);
            });
        });
    }
}

class Tests {
    constructor() {
        this.connection = null;
        this.tests = [];
    }

    connect(ip, port, user, password) {
        this.connection = mysql.createConnection({
            host: ip,
            port: port,
            user: user,
            password: password
        });
        return new Promise((resolve, reject) => {
            this.connection.connect((err) => {
                if (err) {
                    console.error('Error connecting to the database:', err.message);
                    return reject(err);
                }
                resolve();
            });
        });
    }

    disconnect() {
        return new Promise((resolve, reject) => {
            this.connection.end((err) => {
                if (err) {
                    console.error('Error disconnecting from the database:', err.message);
                    return reject(err);
                }
                resolve();
            });
        });
    }

    addTest(query, expectedResults) {
        this.tests.push(new Test(query, expectedResults));
    }

    async runTests() {
        for (const test of this.tests) {
            if (!(await test.run(this.connection))) {
                return false;
            }
        }
        return true;
    }

    readTestsFromFile(filename) {
        const data = fs.readFileSync(filename, 'utf8');
        const lines = data.split('\n');
        let query = null;
        let expectedResults = [];
        for (const line of lines) {
            const trimmedLine = line.trim();
            if (!trimmedLine) {
                if (query) {
                    this.addTest(query, expectedResults);
                    query = null;
                    expectedResults = [];
                }
            } else if (query === null) {
                query = trimmedLine;
            } else {
                expectedResults.push(trimmedLine.split(','));
            }
        }
        if (query) {
            this.addTest(query, expectedResults);
        }
    }
}

(async () => {
    if (process.argv.length < 6) {
        console.error("Usage: node mysql_test.js <ip> <port> <user> <password> <testFile>");
        process.exit(1);
    }

    const tests = new Tests();
    await tests.connect(process.argv[2], parseInt(process.argv[3]), process.argv[4], process.argv[5]);
    tests.readTestsFromFile(process.argv[6]);

    if (!(await tests.runTests())) {
        await tests.disconnect();
        process.exit(1);
    }
    await tests.disconnect();
})();