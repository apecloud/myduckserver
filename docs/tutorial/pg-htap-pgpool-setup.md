This a tutorial to build an HTAP service based on PostgreSQL, MyDuck Server, and [PGPool-II](https://www.pgpool.net/docs/pgpool-II-4.2.5/en/html/index.html).

# Prerequisites

* Install `docker-compose`
    * On MacOS, please run `brew install docker-compose`.
    * On Linux, please do the following:
        * Run `sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose`.
        * And then run `sudo chmod +x /usr/local/bin/docker-compose`.

# Launch the HTAP cluster

Go the root path of this project and run the following commands:

```sh
cd devtools/htap-setup-pg/pgpool2
docker-compose up -d
```

Then you'll get a HTAP cluster. Have fun!

# Connecting

The HTAP service can be accessed by

```sh
docker exec -ti htap-pgpool bash -c "PGPASSWORD=postgres psql -h localhost -U postgres -d postgres"
``` 

And a table `test` with one row data is replicated from the primary PostreSQL to MyDuck server.

```sql
psql (17.1)
Type "help" for help.

postgres=# \d
        List of relations
 Schema | Name | Type  |  Owner
--------+------+-------+----------
 public | test | table | postgres
(1 row)

postgres=# select * from test;
 id | name
----+------
  1 | test
(1 row)
```

# Monitor status

* The status of proxy `PGPool-II` can be retrieved by the built-in statement [SHOW POOL_NODES](https://www.pgpool.net/docs/latest/en/html/sql-show-pool-nodes.html). e.g. You can get the status of the servers by executing the statement `SHOW POOL_NODES;` on the connection to PGPool server.
```sql
postgres=# show pool_nodes;
 node_id | hostname | port | status | pg_status | lb_weight |  role   | pg_role | select_cnt | load_balance_node | replication_delay | replication_state | replication_sync_state | last_status_change  
---------+----------+------+--------+-----------+-----------+---------+---------+------------+-------------------+-------------------+-------------------+------------------------+---------------------
 0       | pgsql    | 5432 | up     | up        | 0.000000  | primary | primary | 1          | false             | 0                 |                   |                        | 2024-11-20 14:24:25
 1       | myduck   | 5432 | up     | up        | 1.000000  | standby | standby | 0          | true              | 288784            | streaming         | async                  | 2024-11-20 14:24:25
(2 rows)
```

* After you connect to the HTAP service, any read statements will be sent to MyDuck Server. MyDuck Server will leverage the power of DuckDB to boost the analytical performance! The counting of the statement routing is also shown in the column `select_cnt` in the result of `SHOW POOL_NODES`.

For instance, before executing a `READ` statement on HTAP service:
```sql
postgres=# show pool_nodes;
 node_id | hostname | port | status | pg_status | lb_weight |  role   | pg_role | select_cnt | load_balance_node | replication_delay | replication_state | replication_sync_state | last_status_change  
---------+----------+------+--------+-----------+-----------+---------+---------+------------+-------------------+-------------------+-------------------+------------------------+---------------------
 0       | pgsql    | 5432 | up     | up        | 0.000000  | primary | primary | 1          | false             | 0                 |                   |                        | 2024-11-20 14:24:25
 1       | myduck   | 5432 | up     | up        | 1.000000  | standby | standby | 0          | true              | 288784            | streaming         | async                  | 2024-11-20 14:24:25
(2 rows)
```

after executing the `READ` statement. As you can see, the `select_cnt` has been increased by 1, indicating that the read statement has been routed to MyDuck server.
```sql
postgres=# select * from test;
 id | name 
----+------
  1 | test
(1 row)

postgres=# show pool_nodes;
 node_id | hostname | port | status | pg_status | lb_weight |  role   | pg_role | select_cnt | load_balance_node | replication_delay | replication_state | replication_sync_state | last_status_change  
---------+----------+------+--------+-----------+-----------+---------+---------+------------+-------------------+-------------------+-------------------+------------------------+---------------------
 0       | pgsql    | 5432 | up     | up        | 0.000000  | primary | primary | 1          | false             | 0                 |                   |                        | 2024-11-20 14:24:25
 1       | myduck   | 5432 | up     | up        | 1.000000  | standby | standby | 1          | true              | 288784            | streaming         | async                  | 2024-11-20 14:24:25
(2 rows)
```

Let's insert a new row and then query the table. Without surprise, the data has been replicated to our MyDuck server.
```sql
postgres=# 
postgres=# insert into test values (2, 'test again');
INSERT 0 1
postgres=# select * from test;
 id |    name    
----+------------
  1 | test
  2 | test again
(2 rows)

postgres=# show pool_nodes;
 node_id | hostname | port | status | pg_status | lb_weight |  role   | pg_role | select_cnt | load_balance_node | replication_delay | replication_state | replication_sync_state | last_status_change  
---------+----------+------+--------+-----------+-----------+---------+---------+------------+-------------------+-------------------+-------------------+------------------------+---------------------
 0       | pgsql    | 5432 | up     | up        | 0.000000  | primary | primary | 1          | false             | 0                 |                   |                        | 2024-11-20 14:24:25
 1       | myduck   | 5432 | up     | up        | 1.000000  | standby | standby | 2          | true              | 48                | streaming         | async                  | 2024-11-20 14:24:25
(2 rows)
```

# Cleanup

You can run `docker-compose down` to clean up all resources after the trial.
