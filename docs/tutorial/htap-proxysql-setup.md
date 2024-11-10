
This is a tutorial to build an HTAP service based on MySQL, MyDuck Server, and ProxySQL.

# Prerequisites

* Install `docker-compose`
    * On MacOS, please run `brew install docker-compose`.
    * On Linux, please do the following:
        * Run `sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose`.
        * And then run `sudo chmod +x /usr/local/bin/docker-compose`.

# Launch the HTAP cluster

Go the root path of this project and run the following commands:

```
cd devtools/htap-setup/proxysql
docker-compose up
```

Then you'll get a HTAP cluster. And an account 'lol' with password 'lol' has been created for connecting. Have fun!

# Connecting
The HTAP service can be accessed by 

```
mysql -h127.0.0.1 -P16033 -ulol -plol
```

# Monitor status

The status of ProxySQL can be checked by connecting to its admin interface:

```sh
mysql -h127.0.0.1 -P16032 -uradmin -pradmin --prompt='ProxySQL Admin> '
```

In the admin interface, you can check server status:

```sh
SELECT * FROM mysql_servers;
SELECT * FROM stats.stats_mysql_connection_pool;
```

ProxySQL will automatically route read queries to MyDuck Server. You can monitor the query distribution by checking:

```sh
SELECT * FROM stats.stats_mysql_commands_counters;
```

# Cleanup

You can run `docker-compose down` to clean up all resources after the trial.