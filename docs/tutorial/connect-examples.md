# Connect with common clients

Default Docker ports:

- MySQL protocol: `13306`
- Postgres protocol: `15432`

```bash
docker run -p 13306:3306 -p 15432:5432 apecloud/myduckserver:latest
```

MySQL-style SQL goes to the MySQL port. DuckDB SQL goes to the Postgres port.

## CLI

```bash
mysql -h127.0.0.1 -P13306 -uroot
```

```bash
psql -h 127.0.0.1 -p 15432 -U postgres
```

macOS MySQL 9 CLI is not supported yet. Use `mysql-client@8.4` if needed.

## Python

```python
import mysql.connector

conn = mysql.connector.connect(host="127.0.0.1", port=13306, user="root", password="")
cur = conn.cursor()
cur.execute("SELECT 1")
print(cur.fetchall())
cur.close()
conn.close()
```

```python
import psycopg

with psycopg.connect("host=127.0.0.1 port=15432 user=postgres dbname=postgres") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        print(cur.fetchall())
```

More Python data-tool examples: [pg-python-data-tools.md](pg-python-data-tools.md).

## Node.js

```javascript
const mysql = require("mysql");

const conn = mysql.createConnection({
  host: "127.0.0.1",
  port: 13306,
  user: "root",
  password: "",
});

conn.query("SELECT 1", (err, rows) => {
  if (err) throw err;
  console.log(rows);
  conn.end();
});
```

```javascript
const { Client } = require("pg");

const client = new Client({
  host: "127.0.0.1",
  port: 15432,
  user: "postgres",
});

await client.connect();
console.log((await client.query("SELECT 1")).rows);
await client.end();
```

## Go

```go
db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:13306)/")
```

```go
db, err := sql.Open("postgres", "host=127.0.0.1 port=15432 user=postgres dbname=postgres sslmode=disable")
```

## Java

```java
Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:13306/", "root", "");
```

```java
Connection conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:15432/postgres", "postgres", "");
```

## R

```r
con <- DBI::dbConnect(RMySQL::MySQL(), host = "127.0.0.1", port = 13306, user = "root", password = "")
```

```r
con <- DBI::dbConnect(RPostgres::Postgres(), host = "127.0.0.1", port = 15432, user = "postgres", dbname = "postgres")
```

Live client checks live under `compatibility/` and run in GitHub Actions, not on a local Docker daemon.
