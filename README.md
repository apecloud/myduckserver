<h1 style="display: flex; align-items: center;">
    <img width="50" alt="duck under dolphin" style="margin-right: 0.2em" src="logo/myduck-logo.png">
    <span>MyDuck Server</span>
</h1>


**MyDuck Server** unlocks serious power for your MySQL & Postgres analytics. Imagine the simplicity of (MySQL|Postgres)’s familiar interface fused with the raw analytical speed of [DuckDB](https://duckdb.org/). Now you can supercharge your analytical queries with DuckDB’s lightning-fast OLAP engine, all while using the tools and dialect you know.

## ❓ Why MyDuck ❓

While MySQL and Postgres are the most popular open-source databases for OLTP, their performances in analytics often fall short. DuckDB, on the other hand, is built for fast, embedded analytical processing. MyDuck Server lets you enjoy DuckDB's high-speed analytics without leaving the (MySQL|Postgres) ecosystem.

With MyDuck Server, you can:

- **Accelerate analytics** by running queries on your MySQL & Postgres data at speeds several orders of magnitude faster 🚀
- **Keep familiar tools**—there's no need to change your existing (MySQL|Postgres)-based data analysis toolchains 🛠️
- **Go beyond MySQL & Postgres syntax** through DuckDB's full power to expand your analytics potential 💥
- **Run DuckDB in server mode** to share a DuckDB instance with your team or among your applications 🌩️
- **Build HTAP systems** by combining (MySQL|Postgres) for transactions with MyDuck for analytics 🔄
- and much more! See below for a full list of feature highlights.

MyDuck Server isn't here to replace MySQL & Postgres — it's here to help MySQL & Postgres users do more with their data. This open-source project gives you a convenient way to integrate high-speed analytics into your workflow, all while embracing the flexibility and efficiency of DuckDB.

## ✨ Key Features

<h1 style="display: flex; align-items: center;">
    <img alt="duck under dolphin" style="margin-right: 0.2em" src="logo/MyDuck.svg">
</h1>

- **Blazing Fast OLAP with DuckDB**: MyDuck stores data in DuckDB, an OLAP-optimized database known for lightning-fast analytical queries. With DuckDB, MyDuck executes queries up to 1000x faster than traditional MySQL & Postgres setups, enabling complex analytics that were impractical with MySQL or Postgres alone.

- **MySQL-Compatible Interface**: MyDuck speaks MySQL wire protocol and understands MySQL syntax, so you can connect to it with any MySQL client and run MySQL-style SQL. MyDuck translates your queries on the fly and executes them in DuckDB.

- **Postgres-Compatible Interface**: MyDuck speaks Postgres wire protocol as well, allowing you to send DuckDB SQL directly with any Postgres client. DuckDB's SQL dialect [closely resembles PostgreSQL](https://duckdb.org/docs/sql/dialect/postgresql_compatibility.html), enabling you to speed up existing Postgres queries with minimal changes.

- **Raw DuckDB Power**: MyDuck's support for raw DuckDB SQL opens up DuckDB’s full analytical capabilities, including [friendly SQL syntax](https://duckdb.org/docs/sql/dialect/friendly_sql.html), [advanced aggregates](https://duckdb.org/docs/sql/functions/aggregates), [accessing remote data sources](https://duckdb.org/docs/data/data_sources), and more. 

- **Zero-ETL**: Just start replication and go! MyDuck can act as a MySQL replica or a Postgres standby that replicates data from your primary server in real-time, so you can start querying immediately. There’s no need to set up complex ETL pipelines.

- **Consistent and Efficient Replication**: Thanks to DuckDB's [solid ACID support](https://duckdb.org/2024/09/25/changing-data-with-confidence-and-acid.html), we've carefully managed transaction boundaries in the replication stream to ensure a **consistent data view** — you'll never see dirty data mid-transaction. Plus, MyDuck's **transaction batching** collects updates from multiple transactions and applies them to DuckDB in batches, significantly reducing write overhead (since DuckDB isn’t designed for high-frequency OLTP writes).

- **HTAP Architecture Support**: MyDuck works well with database proxy tools to enable hybrid transactional/analytical processing setups. You can route DML operations to (MySQL|Postgres) and analytical queries to MyDuck, creating a powerful HTAP architecture that combines the best of both worlds.

- **Seamless Integration with Dump & Copy Utilities**: MyDuck plays well with modern MySQL & Postgres data migration tools, especially the [MySQL Shell](https://dev.mysql.com/doc/mysql-shell/en/) and [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html). For MySQL, you can load data into MyDuck in parallel from a MySQL Shell dump, or leverage the Shell’s `copy-instance` utility to copy a consistent snapshot of your running MySQL server to MyDuck. For Postgres, MyDuck can load data from a `pg_dump` archive.

- **Bulk Upload & Download**: MyDuck supports fast bulk data loading from the client side with the standard MySQL `LOAD DATA LOCAL INFILE` command or the  PostgreSQL `COPY FROM STDIN` command. You can also extract data from MyDuck using the PostgreSQL `COPY TO STDOUT` command.

- **Standalone Mode**: MyDuck can run in standalone mode without replication. In this mode, it is a drop-in replacement for (MySQL|Postgres), but with a DuckDB heart. You can `CREATE TABLE`, transactionally `INSERT`, `UPDATE`, and `DELETE` data, and run blazingly fast `SELECT` queries.

- **DuckDB in Server Mode**: If you aren't interested in MySQL & Postgres but just want to share a DuckDB instance with your team or among your applications, MyDuck is also a great solution. You can deploy MyDuck to a server, connect to it with the Postgres client library in your favorite programming language, and start running DuckDB SQL queries directly.

## 📊 Performance

Typical OLAP queries can run **up to 1000x faster** with MyDuck Server compared to MySQL & Postgres alone, especially on large datasets. Under the hood, it's just DuckDB doing what it does best: processing analytical queries at lightning speed. You are welcome to run your own benchmarks and prepare to be amazed! Alternatively, you can refer to well-known benchmarks like the [ClickBench](https://benchmark.clickhouse.com/) and [H2O.ai db-benchmark](https://duckdblabs.github.io/db-benchmark/) to see how DuckDB performs against other databases and data science tools. Also remember that DuckDB has robust support for transactions, JOINs, and [larger-than-memory query processing](https://duckdb.org/2024/07/09/memory-management.html), which are unavailable in many competing systems and tools.

## 🎯 Roadmap

We have big plans for MyDuck Server! Here are some of the features we’re working on:

- [x] Be compatible with MySQL proxy tools like [ProxySQL](https://proxysql.com/).
- [x] Replicate data from PostgreSQL.
- [ ] Authentication.
- [ ] ...and more! We’re always looking for ways to make MyDuck Server better. If you have a feature request, please let us know by [opening an issue](https://github.com/apecloud/myduckserver/issues/new).


## 🏃‍♂️ Getting Started

### Prerequisites

- **Docker** (recommended) for setting up MyDuck Server quickly.
- MySQL or PostgreSQL CLI clients for connecting and testing your setup.

### Installation

Get a standalone MyDuck Server up and running in minutes using Docker:

```bash
docker run -p 13306:3306 -p 15432:5432 apecloud/myduckserver:latest
```

This setup exposes:

- **Port 13306** for MySQL wire protocol connections.
- **Port 15432** for PostgreSQL wire protocol connections, allowing direct DuckDB SQL.

### Usage

#### Connecting via MySQL client

Connect using any MySQL client to run MySQL-style SQL queries:

```bash
mysql -h127.0.0.1 -P13306 -uroot
```

> [!NOTE]
> MySQL CLI clients version 9.0 and above are not yet supported on macOS. Consider `brew install mysql-client@8.4`.

#### Connecting via PostgreSQL client

For full analytical power, connect to the Postgres port and run DuckDB SQL queries directly:

```bash
psql -h 127.0.0.1 -p 15432 -U postgres
```

### Replicating Data

We have integrated a setup tool in the Docker image that helps replicate data from your primary (MySQL|Postgres) server to MyDuck Server. The tool is available via the `SETUP_MODE` environment variable. In `REPLICA` mode, the container will start MyDuck Server, dump a snapshot of your primary (MySQL|Postgres) server, and start replicating data in real-time.

```bash
docker run \
  -p 13306:3306 \ 
  -p 15432:5432 \
  --privileged \
  --workdir=/home/admin \
  --env=SETUP_MODE=REPLICA \
  --env=SOURCE_DSN="<postgresql|mysql>://<user>:<password>@<host>:<port>/<dbname>"
  --detach=true \
  apecloud/myduckserver:latest
```
`SOURCE_DSN` specifies the connection string to the primary database server, which can be either MySQL or PostgreSQL.

- **MySQL Primary:** Use the MySQL URI scheme, e.g.,  
  `--env=SOURCE_DSN=mysql://root:password@example.com:3306`

- **PostgreSQL Primary:** Use the PostgreSQL URI scheme, e.g.,  
  `--env=SOURCE_DSN=postgres://postgres:password@example.com:5432`

### Connecting to Cloud MySQL & Postgres

MyDuck Server supports setting up replicas from common cloud-based MySQL & Postgres offerings. For more information, please refer to the [replica setup guide](docs/tutorial/replica-setup-rds.md).

### HTAP Setup

With MyDuck's powerful analytics capabilities, you can create an hybrid transactional/analytical processing system where high-frequency data writes are directed to a standard MySQL or Postgres instance, while analytical queries are handled by a MyDuck Server instance. Follow our HTAP setup instructions to easily set up an HTAP demonstration:
* Provisioning a MySQL HTAP cluster based on [ProxySQL](docs/tutorial/mysql-htap-proxysql-setup.md) or [MariaDB MaxScale](docs/tutorial/mysql-htap-maxscale-setup.md).
* Provisioning a PostgreSQL HTAP cluster based on [PGPool-II](docs/tutorial/pg-htap-pgpool-setup.md)

### Query & Load Parquet Files

Looking to load Parquet files into MyDuck Server and start querying? Follow our [Parquet file loading guide](docs/tutorial/load-parquet-files.md) for easy setup.

### Already Using DuckDB?

Already have a DuckDB file? You can seamlessly bootstrap MyDuck Server with it. See our [DuckDB file bootstrapping guide](docs/tutorial/bootstrap.md) for more details.

## 💡 Contributing

Let’s make (MySQL|Postgres) analytics fast and powerful — together!

MyDuck Server is open-source, and we’d love your help to keep it growing! Check out our [CONTRIBUTING.md](CONTRIBUTING.md) for ways to get involved. From bug reports to feature requests, all contributions are welcome!

## 💗 Acknowledgements

MyDuck Server is built on top of a collection of amazing open-source projects, notably:
- [DuckDB](https://duckdb.org/) - The fast in-process analytical database that powers MyDuck Server.
- [go-mysql-server](https://github.com/dolthub/go-mysql-server) - The outstanding MySQL server implementation in Go maintained by [DoltHub](https://www.dolthub.com/team) that MyDuck Server is bulit on. We also draw significant inspiration from [Dolt](https://github.com/dolthub/dolt) and [Doltgres](https://github.com/dolthub/doltgres).
- [Vitess](https://vitess.io/) - Provides the MySQL replication stream used in MyDuck Server.
- [go-duckdb](https://github.com/marcboeker/go-duckdb): An excellent Go driver for DuckDB that works seamlessly.
- [SQLGlot](https://github.com/tobymao/sqlglot) - The ultimate SQL transpiler.

We are grateful to the developers and contributors of these projects for their hard work and dedication to open-source software.

## 📝 License

MyDuck Server is released under the [Apache License 2.0](LICENSE).
