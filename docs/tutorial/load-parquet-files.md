# Query & Load Parquet Files

Imagine you have a large dataset stored in Parquet files. You want to share this data with your team, enabling them to query it using SQL.
However, these files are too large to be stored locally and too slow to download from cloud storage every time.
You can put these files on a server that is accessible to your team and run a MyDuck Server instance on it.
Then, your team can query the dataset easily with either a Postgres or a MySQL client.

Below, we’ll show you how to query and load the `example.parquet` file from the `docs/data/` directory by attaching it into a MyDuck Server container.

## Steps

1. **Run MyDuck Server:**
   ```bash
   docker run -p 13306:3306 -p 15432:5432 \
        -v /path/to/example.parquet:/home/admin/data/example.parquet \
        apecloud/myduckserver:main
   ```

2. **Connect to MyDuck Server using `psql`:**
   ```bash
   psql -h 127.0.0.1 -p 15432 -U mysql
   ```

3. **Query the Parquet file directly:**
   ```sql
   SELECT * FROM '/home/admin/data/example.parquet' LIMIT 10;
   ```

4. **Load the Parquet file into a DuckDB table:**
   ```sql
   CREATE TABLE test_data AS SELECT * FROM '/home/admin/data/example.parquet';
   SELECT * FROM test_data LIMIT 10;
   ```

5. **Query the data with MySQL client & syntax:**
   ```bash
   mysql -h 127.0.0.1 -uroot -P13306 main
   ```
   ```sql
   SELECT * FROM `test_data`;
   ```
