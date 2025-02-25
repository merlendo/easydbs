import easydbs
from sqlmodel import Session, SQLModel, Field, inspect

class Hero(SQLModel, table=True):
    __tablename__ = "hero"
    __table_args__ = {'extend_existing': True}
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
def test_create_tables_sqlite(session: Session):
    sqlite.create_tables(tables_names=["hero"])
    inspector = inspect(session.bind)
    tables = inspector.get_table_names()
    assert "hero" in tables

@postgre
def test_create_tables_postgre(session: Session):
    postgre.create_tables(tables_names=["hero"])
    inspector = inspect(session.bind)
    tables = inspector.get_table_names()
    assert "hero" in tables

@mysql
def test_create_tables_mysql(session: Session):
    mysql.create_tables(tables_names=["hero"])
    inspector = inspect(session.bind)
    tables = inspector.get_table_names()
    assert "hero" in tables

@mariadb
def test_create_tables_mariadb(session: Session):
    mariadb.create_tables(tables_names=["hero"])
    inspector = inspect(session.bind)
    tables = inspector.get_table_names()
    assert "hero" in tables
