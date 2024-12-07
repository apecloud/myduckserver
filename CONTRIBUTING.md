# Contributing to MyDuck Server

Thank you for contributing to MyDuck Server! This guide will help you set up the development environment and run the server.

## Prerequisites

Before getting started, ensure that the following dependencies are installed:

1. **Go**  
   Download and install the latest version of Go by following the [official installation guide](https://go.dev/doc/install).

2. **Python and `sqlglot[rs]` package**  
    MyDuck Server depends on the `sqlglot[rs]` package, which can be installed using `pip3`. You have two options for installation:

    - **Global installation** (use with caution as it may affect system packages):
        ```bash
        pip3 install "sqlglot[rs]" --break-system-packages
        ```

    - **Installation inside a virtual environment** (recommended):
        ```bash
        mkdir -p ~/venv
        python3 -m venv ~/venv/myduck
        source ~/venv/myduck/bin/activate
        pip3 install "sqlglot[rs]"
        ```

    Make sure to activate the virtual environment when you work on the project:
    ```bash
    source ~/venv/myduck/bin/activate
    ```

---

## Build and Run MyDuck Server

### 1. Build the project

To build MyDuck Server, run the following command:

```bash
go get -v
go build -v
```

This will compile the necessary files.

### 2. Start MyDuck Server

Once built, run the server:

```bash
make run
```

### 3. Connect to MyDuck Server

- **Using MySQL Client**:  
  In another terminal window, connect to the MyDuck Server using the MySQL client with the following command:

  ```bash
  mysql -h127.0.0.1 -uroot -P3306
  ```

- **Using PostgreSQL Client**:  
  If you prefer to use a Postgres client, connect using the following command:

  ```bash
  psql -h 127.0.0.1 -p 5432 -U postgres
  ```

---

## Additional Notes

- Ensure that you have all the necessary permissions for the system dependencies (e.g., `pip3` installation with `--break-system-packages`).
- The `make` tool is required to build and run the project. If it's not already installed, you can install it via your package manager (e.g., `brew install make` on macOS).
- If you run into any issues or need help, feel free to open an issue or reach out to the community.