# Guide to Using MyDuck Server with DuckDB

This guide is for users familiar with DuckDB, demonstrating how to use MyDuck Server to accomplish similar tasks. You can connect to MyDuck Server via `psql` to execute SQL queries on DuckDB.

## Example 1: Load Parquet Files

Loading and querying Parquet files is a common use case. Below, we’ll show you how to load `example.parquet` from the `docs/data/` directory.

### Steps

1. **Connect to MyDuck Server using `psql`:**
   ```bash
   psql -h 127.0.0.1 -p 15432 -U mysql
   ```

2. **Load the Parquet file into DuckDB:**
   ```sql
   CREATE TABLE test_data AS SELECT * FROM '/path/to/example.parquet';
   ```

3. **Query the data:**
   ```sql
   SELECT * FROM test_data;
   ```

4. **Access via MySQL client:**
   ```bash
   mysql -h 127.0.0.1 -uroot -P13306
   ```
   ```sql
   USE main;
   SELECT * FROM test_data;
   ```

## Example 2: Attach an Existing DuckDB Database File

To query an existing DuckDB file, you can attach it to MyDuck Server. Here’s how to work with the `example.db` file located in `docs/data/`.

### Steps

1. **Prepare the data directory:**
   ```bash
   mkdir example_data
   cp /path/to/example.db example_data/mysql.db
   ```

2. **Launch MyDuck Server and attach the data directory:**
   ```bash
   docker run \
   -p 13306:3306 \
   -p 15432:5432 \
   --volume=/path/to/example_data:/home/admin/data \
   apecloud/myduckserver:main
   ```

3. **Connect to MyDuck Server and query:**
   ```bash
   # Using psql
   psql -h 127.0.0.1 -p 15432 -U mysql
   
   # Or using MySQL client
   mysql -h 127.0.0.1 -uroot -P13306
   ```
   ```sql
   USE main;
   SELECT * FROM test_data;
   ```