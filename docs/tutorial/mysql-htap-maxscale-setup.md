This a tutorial to build an HTAP service based on MySQL, MyDuck Server, and MariaDB MaxScale.

# Prerequisites

* Install [Docker Compose](https://docs.docker.com/compose/install/)

# Launch the HTAP cluster

Go the root path of this project and run the following commands:

```sh
cd devtools/htap-setup-mysql/maxscale
docker compose up -d --wait
```

Then you'll get a HTAP cluster. And an account 'lol' with password 'lol' has been created for connecting. Have fun!

# Connecting

The HTAP service can be accessed by

```sh
mysql -h127.0.0.1 -P14000 -ulol -plol
``` 

# Monitor status

* You can connect to the replica and execute `SHOW REPLICA STATUS` to check the replication status.

* The status of proxy `MaxScale` can be retrieved by the built-in tool [maxctrl](https://mariadb.com/kb/en/mariadb-maxscale-24-maxctrl/). e.g. You can get the status of the servers by `docker exec maxscale maxctrl list servers`
```bash
% docker exec maxscale maxctrl list servers                                            
┌───────────────┬──────────────────────┬──────┬─────────────┬─────────────────┬──────┬───────────────┐
│ Server        │ Address              │ Port │ Connections │ State           │ GTID │ Monitor       │
├───────────────┼──────────────────────┼──────┼─────────────┼─────────────────┼──────┼───────────────┤
│ mysql-primary │ host.docker.internal │ 3306 │ 0           │ Master, Running │      │ MySQL-Monitor │
├───────────────┼──────────────────────┼──────┼─────────────┼─────────────────┼──────┼───────────────┤
│ myduck-server │ host.docker.internal │ 3307 │ 0           │ Slave, Running  │      │ MySQL-Monitor │
└───────────────┴──────────────────────┴──────┴─────────────┴─────────────────┴──────┴───────────────┘
```

* After you connect to the HTAP service, any read statements will be sent to MyDuck Server. MyDuck Server will leverage the power of DuckDB to boost the analytical performance! you can execute `docker exec maxscale maxctrl show server myduck-server | grep count | grep -v '"count": 0,'` to get the counting of the requests have been sent to the replica. If you want to check the counting of the requests sent to primary node, please replace the `myduck-server` with `mysql-primary` in the former command.

For instance, before executing a `READ` statement on HTAP service:
```bash
% docker exec maxscale maxctrl show server myduck-server | grep count | grep -v '"count": 0,'
│                     │                     "count": 2,              │
```

after executing the `READ` statement:
```bash
% docker exec maxscale maxctrl show server myduck-server | grep count | grep -v '"count": 0,'
│                     │                     "count": 2,              │
│                     │                     "count": 1,              │
```

# Cleanup

You can run `docker compose down` to clean up all resources after the trial.
