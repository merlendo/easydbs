import pytest
import easydbs
from sqlmodel import Session, select, SQLModel, Field, insert


class Hero(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    secret_name: str
    age: int | None = None


sqlite = easydbs.connect(easydbs.SQLITE)
duckdb = easydbs.connect(easydbs.DUCKDB)
postgre = easydbs.connect(
    db_type=easydbs.POSTGRE,
    username="testuser",
    password="testpassword",
    host="localhost",
    port=5433,
    database="testdb",
)
mysql = easydbs.connect(
    db_type=easydbs.MYSQL,
    username="testuser",
    password="testpassword",
    host="localhost",
    port=3306,
    database="testdb",
)
mariadb = easydbs.connect(
    db_type=easydbs.MARIADB,
    username="testuser",
    password="testpassword",
    host="localhost",
    port=3307,
    database="testdb",
)


@sqlite
def test_insert_sqlite(session: Session):
    sqlite.create_tables()
    hero = Hero(name="Peter Parker", secret_name="Spider-Man", age=29)
    session.add(hero)
    session.commit()
    result = session.exec(select(Hero)).first()
    assert result is not None
    assert result.name == "Peter Parker"
    assert result.secret_name == "Spider-Man"
    assert result.age == 29


@duckdb
def test_insert_duckdb(session: Session):
    session.exec("""
    CREATE TABLE hero (
        id INTEGER, 
        name STRING, 
        secret_name STRING,
        age INTEGER)
    """)
    session.exec(
        "INSERT INTO hero VALUES (:id, :name, :secret_name, :age)", 
        params={"id":1, "name":"Peter Parker", "secret_name":"Spider-Man", "age":29}
    )
    session.commit()
    result = session.exec("SELECT * FROM hero").first()
    assert result is not None
    assert result == (1, "Peter Parker", "Spider-Man", 29)


@postgre
def test_insert_postgre(session: Session):
    postgre.create_tables()
    hero = Hero(name="Peter Parker", secret_name="Spider-Man", age=29)
    session.add(hero)
    session.commit()
    result = session.exec(select(Hero)).first()
    assert result is not None
    assert result.name == "Peter Parker"
    assert result.secret_name == "Spider-Man"
    assert result.age == 29


@mysql
def test_insert_mysql(session: Session):
    mysql.create_tables()
    hero = Hero(name="Peter Parker", secret_name="Spider-Man", age=29)
    session.add(hero)
    session.commit()
    result = session.exec(select(Hero)).first()
    assert result is not None
    assert result.name == "Peter Parker"
    assert result.secret_name == "Spider-Man"
    assert result.age == 29


@mariadb
def test_insert_mariadb(session: Session):
    mariadb.create_tables()
    hero = Hero(name="Peter Parker", secret_name="Spider-Man", age=29)
    session.add(hero)
    session.commit()
    result = session.exec(select(Hero)).first()
    assert result is not None
    assert result.name == "Peter Parker"
    assert result.secret_name == "Spider-Man"
    assert result.age == 29
