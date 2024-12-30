### Managing Databases in MyDuck Server

MyDuck Server offers basic database operations similar to PostgreSQL. Each database in MyDuck Server is represented as a single DuckDB data file. As a result, all database operations are mapped to corresponding file operations. Below are the key commands for managing databases in MyDuck Server:

#### 1. `CREATE DATABASE`
The `CREATE DATABASE db_name` command creates a new database by generating a file named `db_name.db`. This database is then attached to the current DuckDB client session.

**Example:**
```sql
CREATE DATABASE my_database;
```

This will create a new database file `my_database.db` and attach it to the current session.

#### 2. `DROP DATABASE`
The `DROP DATABASE db_name` command detaches the specified database from the current session and deletes the associated database file (`db_name.db`).

**Example:**
```sql
DROP DATABASE my_database;
```

This will detach `my_database` from the current session and permanently delete the file `my_database.db` from storage.

#### 3. `USE DATABASE`
The `USE db_name` command allows you to switch the current session to a different database. After executing this command, all subsequent operations will be performed on the specified database.

**Example:**
```sql
USE my_database;
```

This command switches the current session to the `my_database` database, and all further queries will be executed on it.

---

### Important Notes

- This feature is available only through the PostgreSQL protocol.
- To backup your database to object storage or restore it from a backup file, please refer to the [Backup and Restore Guide](./backup-restore.md).