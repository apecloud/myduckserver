import io

import pandas as pd
import pyarrow as pa
import psycopg

# Create a pandas DataFrame
data = {
    'id': [1, 2, 3],
    'num': [100, 200, 300],
    'data': ['aaa', 'bbb', 'ccc']
}
df = pd.DataFrame(data)

# Convert the DataFrame to an Arrow Table
table = pa.Table.from_pandas(df)

with psycopg.connect("dbname=postgres user=postgres host=127.0.0.1 port=5432", autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS test CASCADE")
        cur.execute("CREATE SCHEMA test")

        # Create a new table
        cur.execute("""
            CREATE TABLE test.tb1 (
                id integer PRIMARY KEY,
                num integer,
                data text)
            """)

        # Use psycopg to write the DataFrame to MyDuck Server
        output_stream = io.BytesIO()
        with pa.ipc.RecordBatchStreamWriter(output_stream, table.schema) as writer:
            writer.write_table(table)
        with cur.copy("COPY test.tb1 FROM STDIN (FORMAT arrow)") as copy:
            copy.write(output_stream.getvalue())

        # Copy the data from MyDuck Server back into a pandas DataFrame using Arrow format
        arrow_data = io.BytesIO()
        with cur.copy("COPY test.tb1 TO STDOUT (FORMAT arrow)") as copy:
            for block in copy:
                arrow_data.write(block)

        # Read the Arrow data into a pandas DataFrame
        with pa.ipc.open_stream(arrow_data.getvalue()) as reader:
            df_from_pg = reader.read_pandas()
            df = df.astype({'id': 'int64', 'num': 'int64'})
            df_from_pg = df_from_pg.astype({'id': 'int64', 'num': 'int64'})
            # Compare the original DataFrame with the DataFrame from PostgreSQL
            assert df.equals(df_from_pg), "DataFrames are not equal"
