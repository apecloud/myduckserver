# Tutorial: Accessing MyDuck Server with psycopg, pyarrow, and polars

## 0. Connecting to MyDuck Server using psycopg

`psycopg` is a popular PostgreSQL adapter for Python. Here is how you can connect to MyDuck Server using `psycopg`:

```python
import psycopg

with psycopg.connect("dbname=postgres user=postgres host=127.0.0.1 port=5432", autocommit=True) as conn:
    with conn.cursor() as cur:
        ...
```

## 1. Using COPY Operation for Direct Interaction

The `COPY` command in PostgreSQL is a powerful tool for bulk data transfer. Here is how you can use it with the `psycopg` library to interact directly with MyDuck Server:

### Writing Data Directly

```python
with cur.copy("COPY test.tb1 (id, num, data) FROM STDIN") as copy:
    copy.write(b"1\t100\taaa\n")
```

### Writing Data Row by Row

```python
with cur.copy("COPY test.tb1 (id, num, data) FROM STDIN") as copy:
    copy.write_row((1, 100, "aaa"))
```

### Reading Data Directly

```python
with cur.copy("COPY test.tb1 TO STDOUT") as copy:
    for block in copy:
        print(block)
```

### Reading Data Row by Row

```python
with cur.copy("COPY test.tb1 TO STDOUT") as copy:
    for row in copy.rows():
        print(row)
```

## 2. Importing and Exporting Data in [Arrow](https://arrow.apache.org/) Format

The `pyarrow` package allows efficient data interchange between DataFrame libraries and MyDuck Server. Here is how to import and export data in Arrow format:

### Creating a pandas DataFrame and Converting to Arrow Table

```python
import pandas as pd
import pyarrow as pa

data = {
    'id': [1, 2, 3],
    'num': [100, 200, 300],
    'data': ['aaa', 'bbb', 'ccc']
}
df = pd.DataFrame(data)
table = pa.Table.from_pandas(df)
```

### Writing Data to MyDuck Server in Arrow Format

```python
import io

output_stream = io.BytesIO()
with pa.ipc.RecordBatchStreamWriter(output_stream, table.schema) as writer:
    writer.write_table(table)
with cur.copy("COPY test.tb1 FROM STDIN (FORMAT arrow)") as copy:
    copy.write(output_stream.getvalue())
```

### Reading Data from MyDuck Server in Arrow Format

```python
arrow_data = io.BytesIO()
with cur.copy("COPY test.tb1 TO STDOUT (FORMAT arrow)") as copy:
    for block in copy:
        arrow_data.write(block)
```

### Deserializing Arrow Data to Arrow DataFrame

```python
with pa.ipc.open_stream(arrow_data.getvalue()) as reader:
    arrow_df = reader.read_all()
    print(arrow_df)
```

### Deserializing Arrow Data to pandas DataFrame

```python
with pa.ipc.open_stream(arrow_data.getvalue()) as reader:
    pandas_df = reader.read_pandas()
    print(pandas_df)
```

## 3. Using Polars to Process DataFrames

[Polars](https://github.com/pola-rs/polars) is a fast DataFrame library that can work with Arrow data. Here is how to use Polars to read Arrow or pandas dataframes:

### Converting Arrow DataFrame to Polars DataFrame

```python
import polars as pl

polars_df = pl.from_arrow(arrow_df)
```

### Converting pandas DataFrame to Polars DataFrame

```python
polars_df = pl.from_pandas(pandas_df)
```
