# easydbs

## Basic usage
```python
import easydbs

conn = easydbs.connect('sqlite', 'app.db')
cursor = conn.cursor()
result = cursor.execute('SELECT * FROM my_table').fetchall()
for row in result:
    print(row)
```

## Use the connection as a decorator
```python
from sqlalchemy import Table, select
from sqlalchemy.orm import Session

import easydbs

cm = easydbs.ConnectionManager()

sqlite_db = cm.add_connection(db_type='sqlite', db_name='app.db')


@sqlite_db
def read_table(session: Session, table_name: str):
    my_table = Table(table_name, sqlite_db.metadata,
                     autoload_with=sqlite_db.engine)
    stmt = select(my_table)
    result = session.execute(stmt)
    for row in result:
        print(row)

read_table('my_table')
```

## Multiple connections
We can insert on a table in several database.

```python
import easydbs
from sqlalchemy import Table

cm = easydbs.ConnexionManager()

cm.add_connection(db_type='sqllite', db_name= 'app.db')
cm.add_connection(db_type="mssql",
                  database="ma_base_mssql",
                  username="sa",
                  password="mon_password",
                  host="localhost",
                  port="1433")

def insert_tables(table_name: str):
    for conn in cm.connections():
        with conn.session() as session:
            my_table = Table(table_name, conn.metadata, autoload_with=conn.engine)
            new_row = my_table(id=1, name="Foo")
            conn.session.add(new_row)
            conn.session.commit()
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


## You can still use a connection with standard SQL
```python
import easydbs

cm = easydbs.ConnexionManager()

sqlite_db = cm.add_connection(db_type='sqllite', db_name= 'app.db')
result = sqlite_db.execute('SELECT * FROM my_table').fetchall()
for row in result:
    print(row)
sqlite_db.close()
```


