# MyDuck Server

**MyDuck Server** unlocks serious power for your MySQL analytics. Imagine the simplicity of MySQL’s familiar interface fused with the raw analytical speed of [DuckDB](https://duckdb.org/). Now you can supercharge your MySQL queries with DuckDB’s lightning-fast OLAP engine, all while using the tools and syntax you know.

## ❓ Why MyDuck Server?

While MySQL is a popular go-to choice for OLTP, its performance in analytics often largely lags. DuckDB, on the other hand, is built for fast, embedded analytical processing. **MyDuck Server** lets you enjoy DuckDB's high-speed analytics without leaving the MySQL ecosystem.

With MyDuck Server, you can:

- **Accelate MySQL analytics** by running analytical queries on your MySQL data at speeds several orders of magnitude faster 🚀
- **Keep familiar tools**—there’s no need to change your existing MySQL-based data analysis toolchains 🛠️
- **Go beyond MySQL syntax** through DuckDB’s full power to expand your analytics potential 💥
- **Run DuckDB in server mode** to share a DuckDB instance with your team or among your applications 🌩️
- and much more! See below for a full list of feature highlights.

MyDuck Server isn’t here to replace MySQL—it’s here to help MySQL users do more with their data. This open-source project gives you a convenient way to integrate high-speed analytics into your MySQL workflow, all while embracing the flexibility and efficiency of DuckDB.

## ✨ Key Features

- **Blazing Fast OLAP with DuckDB**: MyDuck stores data in DuckDB, an OLAP-optimized database known for lightning-fast analytical queries. With DuckDB, MyDuck performs complex queries up to 1000x faster than traditional MySQL setups, enabling complex analytics that were impractical with MySQL alone.

- **MySQL-Compatible Interface**: MyDuck speaks MySQL wire protocol and understands MySQL syntax, so you can connect to it with any MySQL client and run MySQL-style SQL. MyDuck translates your MySQL queries on the fly and executes them in DuckDB. Connect your favorite data visualization tools, BI platforms, and reporting tools to MyDuck without any changes, and enjoy the speed boost.

- **Zero-ETL**: Just `START REPLICA` and go! MyDuck replicates data from your primary MySQL server in real-time, so you can start querying immediately. There’s no need to set up complex ETL pipelines.

- **Consistent and Efficient Replication**: Thanks to DuckDB's [solid ACID support](https://duckdb.org/2024/09/25/changing-data-with-confidence-and-acid.html), we’ve carefully managed transaction boundaries in the replication stream to ensure **consistent data view** — you’ll never see dirty data mid-transaction. Plus, MyDuck’s **transaction batching** collects updates from multiple transactions and applies them to DuckDB in batches, significantly reducing write overhead (since DuckDB isn’t designed for high-frequency OLTP writes).

- **Raw DuckDB Power**: MyDuck also offers a Postgres-compatible port, allowing you to send DuckDB SQL directly. This opens up DuckDB’s full analytical capabilities, including [friendly SQL syntax](https://duckdb.org/docs/sql/dialect/friendly_sql.html), [advanced aggregates](https://duckdb.org/docs/sql/functions/aggregates), [accessing remote data sources](https://duckdb.org/docs/extensions/httpfs/s3api.html#reading), and more. 

- **DuckDB in Server Mode**: If you aren't interested in MySQL but just want to share a DuckDB instance with your team, MyDuck is also a great solution. You can deploy MyDuck to a server, ignore all the MySQL stuff, and connect to it with any PostgreSQL client.

- **Seamless Integration with MySQL Dump & Copy Tools**: MyDuck plays perfectly with modern MySQL tools, especially the [MySQL Shell](https://dev.mysql.com/doc/mysql-shell/en/), the official advanced MySQL client. You can load data into MyDuck in parallel from a MySQL Shell dump, and even leverage the Shell’s `copy-instance` utility to copy a consistent snapshot of your running MySQL server to MyDuck.

- **Bulk Data Loading**: MyDuck supports fast bulk data loading from the client side with the standard MySQL `LOAD DATA LOCAL INFILE` command or the  PostgreSQL `COPY FROM STDIN` command.

- **Standalone Mode**: MyDuck can run in standalone mode, without MySQL replication. In this mode, it is a drop-in replacement for MySQL, but with a DuckDB heart. You can `CREATE TABLE`, transactionally `INSERT`, `UPDATE` and `DELETE` data, and run blazingly-fast `SELECT` queries.

## 📊 Performance

Typical OLAP queries can run **up to 1000x faster** with MyDuck Server compared to MySQL alone, especially on large datasets. Under the hood, it's just DuckDB doing what it does best: processing analytical queries at lightning speed. You are welcome to run your own benchmarks and prepare to be amazed! Alternatively, you can refer to well-known benchmarks like the [ClickBench](https://benchmark.clickhouse.com/) and [H2O.ai db-benchmark](https://duckdblabs.github.io/db-benchmark/) to see how DuckDB performs against other databases and data science tools. Also remember that DuckDB has solid support for JOINs and [larger-than-memory query processing](https://duckdb.org/2024/07/09/memory-management.html), which are not available in many competing systems/tools.


## 🏃‍♂️ Getting Started

### Prerequisites

- **Docker** (recommended) for setting up MyDuck Server quickly.
- MySQL or PostgreSQL clients for connecting and testing your setup.

### Installation

Get a standalone MyDuck Server up and running in minutes using Docker:

```bash
docker run -p 13306:3306 -p 15432:5432 apecloud/myduckserver
```

This setup exposes:

- **Port 13306** for MySQL wire protocol connections.
- **Port 15432** for PostgreSQL wire protocol connections, allowing direct DuckDB SQL.

### Usage

#### Connecting via MySQL

Connect using any MySQL client to run MySQL-style SQL queries:

```bash
mysql -h localhost -P 13306 -u root -p
```

#### Connecting via PostgreSQL

For full analytical power, connect using the PostgreSQL-compatible port and write DuckDB SQL directly:

```bash
psql -h localhost -p 15432 -U postgres
```

### Replicating Data

We have integrated in the Docker image an setup tool that help replicate data from your MySQL server to MyDuck Server. You can run the tool in standalone mode or combined with MyDuck Server. The tool is available via the `SETUP_MODE` environment variable.

### REPLICA_ONLY

In `REPLICA_ONLY` mode, only the replica setup tool runs. The container will exit after the setup completes.

```bash
docker run \
  --network=host \
  --privileged \
  --workdir=/home/admin \
  --env=SETUP_MODE=REPLICA_ONLY \
  --env=MYSQL_HOST=<mysql_host> \
  --env=MYSQL_PORT=<mysql_port> \
  --env=MYSQL_USER=<mysql_user> \
  --env=MYSQL_PASSWORD=<mysql_password> \
  --detach=true \
  apecloud/myduckserver:latest
```

### COMBINED

In `COMBINED` mode, MyDuckServer starts first, followed by the replica setup tool.

```bash
docker run \
  --network=host \
  --privileged \
  --workdir=/home/admin \
  --env=SETUP_MODE=combined \
  --env=MYSQL_HOST=<mysql_host> \
  --env=MYSQL_PORT=<mysql_port> \
  --env=MYSQL_USER=<mysql_user> \
  --env=MYSQL_PASSWORD=<mysql_password> \
  --detach=true \
  apecloud/myduckserver:latest
```


## 💡 Contributing

Let’s make MySQL analytics fast and powerful—together!

MyDuck Server is open-source, and we’d love your help to keep it growing! Check out our [CONTRIBUTING.md](CONTRIBUTING.md) for ways to get involved. From bug reports to feature requests, all contributions are welcome!

## 💗 Acknowledgements

MyDuck Server is built on top of the a collection of amazing open-source projects, notably:
- [DuckDB](https://duckdb.org/) - The fast in-process analytical database that powers MyDuck Server.
- [go-mysql-server](https://github.com/dolthub/go-mysql-server) - The excellent MySQL server implementation in Go maintained by [DoltHub](https://www.dolthub.com/team) that MyDuck Server is based on.
- [Vitess](https://vitess.io/) - Provides the MySQL replication stream subscriber used in MyDuck Server.

We are grateful to the developers and contributors of these projects for their hard work and dedication to open-source software.

## 📝 License

MyDuckServer is released under the [Apache License 2.0](LICENSE). 
