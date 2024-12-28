# easydbs
Easydbs is a python module for people who are to lazy to learn sqlalchemy.  
It's goal is to do simple interactions with relational database. (See Supported Databases.md).  
Easydbs try to be PEP249 complient. [Read more.](https://peps.python.org/pep-0249/)

## Basic usage
Like any pep249 complient python api. You can use methods like connect, cursor, commit, rollback etc...
```python
import easydbs

conn = easydbs.connect('sqlite', 'app.db')
cursor = conn.cursor()
result = cursor.execute('SELECT * FROM my_table').fetchall()
for row in result:
    print(row)
conn.close()
```

## Use the connection as a decorator
You can use the connection object as a decorator.  
You'll have to pass a cursor parameter that will be automaticaly close after the end of the function.
This will work with sync and async functions.

```python
import easydbs

sqlite = easydbs.connect(db_type='sqlite', db_name='app.db')

@sqlite
def read_table(cursor: easydbs.connection.Cursor, table_name: str):
    cursor.execute(f'SELECT * FROM {table_name}')
    for row in cursor.fetchall():
        print(row)

read_table('my_table')
```



## Multiple connections
We can insert on a table in several database.

```python
import easydbs

cm = easydbs.ConnexionManager()

cm.add_connection(db_type='sqllite', db_name= 'app.db')
cm.add_connection(db_type="mssql",
                  database="ma_base_mssql",
                  username="sa",
                  password="mon_password",
                  host="localhost",
                  port="1433")

for conn in cm.connections():
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO my_table VALUES (1, 'foo')")
        conn.commit()
```

## Simpler use with pandas
```python
import pandas as pd
import easydbs

cm = easydbs.ConnexionManager()

conn = cm.add_connection(db_type='sqllite', db_name= 'app.db')
df = pd.read_sql('table_name', engine=conn.engine)
conn.close()
```
