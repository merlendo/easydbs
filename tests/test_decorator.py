import easydbs
import pytest
from sqlmodel import Session
import asyncio

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
def test_decorator_sqlite(session: Session):
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


@duckdb
def test_decorator_duckdb(session: Session):
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


@postgre
def test_decorator_postgre(session: Session):
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


@mysql
def test_decorator_mysql(session: Session):
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


@mariadb
def test_decorator_mariadb(session: Session):
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


@sqlite
@pytest.mark.asyncio
async def test_decorator_sqlite_async(session: Session):
    await asyncio.sleep(0.001)
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)

@duckdb
@pytest.mark.asyncio
async def test_decorator_duckdb_async(session: Session):
    await asyncio.sleep(0.001)
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)

@postgre
@pytest.mark.asyncio
async def test_decorator_postgre_async(session: Session):
    await asyncio.sleep(0.001)
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)

@mysql
@pytest.mark.asyncio
async def test_decorator_mysql_async(session: Session):
    await asyncio.sleep(0.001)
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)

@mariadb
@pytest.mark.asyncio
async def test_decorator_mariadb_async(session: Session):
    await asyncio.sleep(0.001)
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)
