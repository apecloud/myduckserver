# Bootstrapping from an existing DuckDB file

Given an existing DuckDB file, it is possible to bootstrap MyDuck Server with it or serve it with MyDuck Server.
Here’s how to work with the `example.db` file located in `docs/data/`.

### Steps

1. **Prepare the data directory:**
   ```bash
   mkdir example_data
   cp /path/to/example.db example_data/mysql.db # IMPORTANT: The attached filename must be `mysql.db`
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
   # Using psql client & DuckDB syntax
   psql -h 127.0.0.1 -p 15432 -U mysql <<EOF
   SELECT * FROM "test_data";
   EOF

   # Or using MySQL client & syntax
   mysql -h 127.0.0.1 -uroot -P13306 main <<EOF
   SELECT * FROM `test_data`;
   EOF
   ```