# easydbs
Easydbs is a connection manager for people who are too lazy to learn sqlalchemy.  
It's goal is to do simple interactions with relational database. (See Supported Databases.md).  
This module is made for people that use sqlmodel in their fastapi application and want to manage several database connections. 
You can have connections to multiple databases and create a session using your connection as a python decorator.   
Is this module usefull ? Not really, but it was created before knowing that sqlmodel existed. [Check it out](https://sqlmodel.tiangolo.com/).  
Easydbs tries also to be like any PEP249 complient database api. [Read more.](https://peps.python.org/pep-0249/)

## Basic usage
Define the models of your table with sqlmodel.
```python
from sqlmodel import SQLModel, Field

class Hero(SQLModel, table=True):  
    id: int | None = Field(default=None, primary_key=True)  
    name: str  
    secret_name: str  
```

Select all the row of your table.
```python
import easydbs
from sqlmodel import select

sqlite = easydbs.connect(easydbs.SQLITE, database="app.db")
with sqlite.session() as session:
    results = session.exec(select(Hero)).all()
    for row in results:
        print(row)
```

## Use the connection as a decorator
You can use the connection object as a decorator.  
You'll have to pass a session parameter that will be automaticaly close after the end of the function.
This will work with sync and async functions.

```python
import easydbs
from sqlmodel import Session

sqlite = easydbs.connect(easydbs.SQLITE, database="app.db")

@sqlite
def insert_hero(session: Session, hero: Hero):
    session.add(hero)
    session.commit()

hero = Hero(id=1, name="Peter Parker", secret_name="Spiderman")
insert_hero(hero)
```

## Create tables
```python
import easydbs

sqlite = easydbs.connect(easydbs.SQLITE, database="app.db")

sqlite.create_tables(tables_names=["hero"]) # Create hero tables if not exists.
sqlite.create_tables() # Create all tables defined in SQLModel.
```

## Multiple connections
We can easily manage several connections.  
You can use the connection manager or use the function `easydbs.connect`. The connection will be automaticaly added to the connection manager.

```python
import easydbs

cm = easydbs.ConnectionManager()

easydbs.connect(db_type=easydbs.SQLITE, database= 'app.db')
easydbs.connect(
    db_type=easydbs.MYSQL,
    username="testuser",
    password="testpassword",
    host="localhost",
    port=3306,
    database="testdb",
)

for conn in cm.connections():
    conn.create_tables(tables_names=["hero"])
    with conn.session() as session:
        hero = Hero(name="Peter Parker", secret_name="Spiderman")
        session.add(hero)
        session.commit()
```

## Use with pandas
Because the connections are sqlalchemy connection, you can use them with pandas or polars.

```python
import pandas as pd
import easydbs

sqlite = easydbs.connect(easydbs.SQLITE, database="app.db")

df = pd.read_sql('hero', engine=sqlite.engine)
```

## Use easydbs connections like an standard python database api.
Like any pep249 complient python api. You can use methods like connect, cursor, commit, rollback etc...
```python
import easydbs

sqlite = easydbs.connect(easydbs.SQLITE, database="app.db")
cursor = conn.cursor()
result = cursor.execute("INSERT INTO hero VALUES (2, 'Bruce Wayne', 'Batman')")
sqlite.commit()
sqlite.close()
```
