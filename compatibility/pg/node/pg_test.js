const { Client } = require('pg');
const fs = require('fs');

class Test {
    constructor(query, expectedResults) {
        this.query = query;
        this.expectedResults = expectedResults;
    }

    async run(client) {
        try {
            console.log(`Running test: ${this.query}`);
            const res = await client.query(this.query);
            if (res.rows.length === 0) {
                console.log("Returns 0 rows");
                return this.expectedResults.length === 0;
            }
            if (res.fields.length !== this.expectedResults[0].length) {
                console.error(`Expected ${this.expectedResults[0].length} columns, got ${res.fields.length}`);
                return false;
            }
            for (let i = 0; i < res.rows.length; i++) {
                const row = res.rows[i];
                const expectedRow = this.expectedResults[i];
                for (let j = 0; j < expectedRow.length; j++) {
                    const columnName = res.fields[j].name;
                    if (String(row[columnName]) !== String(expectedRow[j])) {
                        console.error(`Expected: ${String(expectedRow[j])}, got: ${String(row[columnName])}`);
                        return false;
                    }
                }
            }
            console.log(`Returns ${res.rows.length} rows`);
            if (res.rows.length !== this.expectedResults.length) {
                console.error(`Expected ${this.expectedResults.length} rows`);
                return false;
            }
            return true;
        } catch (err) {
            console.error(err.message);
            return false;
        }
    }
}

class Tests {
    constructor() {
        this.client = null;
        this.tests = [];
    }

    async connect(ip, port, user, password) {
        this.client = new Client({
            host: ip,
            port: port,
            user: user,
            password: password,
            database: 'postgres'
        });
        await this.client.connect();
    }

    async disconnect() {
        await this.client.end();
    }

    addTest(query, expectedResults) {
        this.tests.push(new Test(query, expectedResults));
    }

    async runTests() {
        for (const test of this.tests) {
            if (!(await test.run(this.client))) {
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
        console.error("Usage: node pg_test.js <ip> <port> <user> <password> <testFile>");
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