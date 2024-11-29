# **Connecting to MyDuck Server using Ibis**

[ibis](https://ibis-project.org/) offers an efficient way to handle data analysis tasks without loading the entire dataset into memory.This tutorial will guide you through setting up a connection using Ibis and performing basic operations with MyDuck Server.

### Steps

1. **Installing ibis**

   ```
   //use conda 
   conda install -c conda-forge ibis-mysql
   //use pip
   pip install 'ibis-framework[mysql]'
   ```

2. **Run MyDuck Server:**

   ```
   docker run -p 13306:3306 -p 15432:5432 apecloud/myduckserver:latest
   ```

3. **Connecting to the Myduck Server**

   ```
   import ibis
   host = '127.0.0.1'
   port = 13306  
   user = 'root'
   password = ''
   database = 'mydb' #should first create the database mydb.
   con = ibis.mysql.connect(
       host=host,
       port=port,
       user=user,
       password=password,
       database=database
   )
   ```

4. **Executing Queries**

   ```
   import ibis
   
   table_name = 'persistent_temp_table'
   schema = ibis.schema([('id', 'int32'), ('value', 'double')])
   
   if table_name not in con.list_tables():
       con.create_table(table_name, schema=schema)
   
   con.insert(table_name, [{'id': 1, 'value': 10.5}])
   
   table = con.table(table_name)
   result = table.execute()
   print(result)
   ```