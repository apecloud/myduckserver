This a tutorial to build a HTAP based on MySQL, MyDuck Server, and MariaDB MaxScale.

# Prerequisites

* Install `docker-compose`
    * On MacOS, please run `brew install docker-compose`.
    * On Linux, please do the following:
        * Run `sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose`.
        * And then run `sudo chmod +x /usr/local/bin/docker-compose`.

* Go to the directory `devtools/htap-setup` under the root path of this project: `cd devtools/htap-setup/`

# Launch the HTAP cluster

Just run `bash startup.sh`. Then you'll get a HTAP cluster. And an account 'lol' with password 'lol' has been created for connecting. Have fun!

# Connecting

* The primary MySQL instance can be connected with `mysql -h127.0.0.1 -P3306 -ulol -plol`.
* The replica(i.e. MyDuck Server) can be connected with `mysql -h127.0.0.1 -P3307 -ulol -plol`.
* The HTAP service can be accessed by `mysql -h127.0.0.1 -P14000 -ulol -plol`.

# Monitor status

* You can connect to the replica and execute `SHOW REPLICA STATUS` to check the replication status.

* The status of proxy `MaxScale` can be retrieved by the built-in tool [maxctrl](https://mariadb.com/kb/en/mariadb-maxscale-24-maxctrl/). e.g. You can get the status of the servers by `docker exec maxscale maxctrl list servers`
```bash
% docker exec maxscale maxctrl list servers                                            
┌─────────┬──────────────────────┬──────┬─────────────┬─────────────────┬──────┬───────────────┐
│ Server  │ Address              │ Port │ Connections │ State           │ GTID │ Monitor       │
├─────────┼──────────────────────┼──────┼─────────────┼─────────────────┼──────┼───────────────┤
│ server1 │ host.docker.internal │ 3306 │ 0           │ Master, Running │      │ MySQL-Monitor │
├─────────┼──────────────────────┼──────┼─────────────┼─────────────────┼──────┼───────────────┤
│ server2 │ host.docker.internal │ 3307 │ 0           │ Slave, Running  │      │ MySQL-Monitor │
└─────────┴──────────────────────┴──────┴─────────────┴─────────────────┴──────┴───────────────┘
```

* After you connect to the HTAP service, any read statements will be sent to MyDuck Server. MyDuck Server will leverage the power of DuckDB to boost the analytical performance! you can execute `docker exec maxscale maxctrl show server server2 | grep count | grep -v '"count": 0,'` to get the counting of the requests have been sent to the replica. If you want to check the counting of the requests sent to primary node, please replace the `server2` with `server1` in the former command.

For instance, before executing a `READ` statement on HTAP service:
```bash
% docker exec maxscale maxctrl show server server2 | grep count | grep -v '"count": 0,'
│                     │                     "count": 1,              │
│                     │                     "count": 1,              │
```

after executing the `READ` statement:
```bash
% docker exec maxscale maxctrl show server server2 | grep count | grep -v '"count": 0,'
│                     │                     "count": 1,              │
│                     │                     "count": 2,              │
```

# Cleanup

You can run `bash cleanup.sh` to cleanup all the testing data after the trial.