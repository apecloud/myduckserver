# Tutorial: Accessing MyDuck Server with PostgreSQL using psycopg, pyarrow, and polars

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

## 2. Importing and Exporting Data in pyarrow Format

`pyarrow` allows efficient data interchange between pandas DataFrames and MyDuck Server. Here is how to import and export data in `pyarrow` format:

### Creating a pandas DataFrame and Converting to Arrow Table

```python
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
    print(arrow_data.getvalue())
```

### Converting Arrow Data to Arrow DataFrame

```python
with pa.ipc.open_stream(arrow_data.getvalue()) as reader:
    arrow_df = reader.read_all()
    print(arrow_df)
```

### Converting Arrow Data to Pandas DataFrame

```python
with pa.ipc.open_stream(arrow_data.getvalue()) as reader:
    pandas_df = reader.read_pandas()
    print(pandas_df)
```

## 3. Using polars to Convert pyarrow Format Data

`polars` is a fast DataFrame library that can work with `pyarrow` data. Here is how to use `polars` to convert `pyarrow` format data:

### Converting Pandas DataFrame to polars DataFrame

```python
polars_df = pl.from_pandas(pandas_df)
```

### Converting Arrow DataFrame to polars DataFrame

```python
polars_df = pl.from_arrow(arrow_df)
```