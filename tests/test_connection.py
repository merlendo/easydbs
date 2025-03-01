import pytest
import easydbs
from . import skip_if_arm

# Connections strings
CONNECTION_STRING = {
    "postgresql": "postgresql://testuser:testpassword@localhost:5433/testdb",
    "mysql": "mysql+pymysql://testuser:testpassword@localhost:3306/testdb",
    "mariadb": "mysql+pymysql://testuser:testpassword@localhost:3307/testdb",
    "sqlite": "sqlite:///:memory:",
    "duckdb": "duckdb:///:memory:",
}

DATABASES = [
    (
        easydbs.POSTGRE,
        None,
        "testuser",
        "testpassword",
        "localhost",
        5433,
        "testdb",
        None,
    ),
    (
        easydbs.MYSQL,
        None,
        "testuser",
        "testpassword",
        "localhost",
        3306,
        "testdb",
        None,
    ),
    (
        easydbs.MARIADB,
        None,
        "testuser",
        "testpassword",
        "localhost",
        3307,
        "testdb",
        None,
    ),
]


@pytest.mark.parametrize(
    "db_type, connection_string, username, password, host, port, database, query",
    DATABASES,
    ids=["PostgreSQL", "MySQL", "MariaDB"],
)
def test_connect(
    db_type, connection_string, username, password, host, port, database, query
):
    conn = easydbs.connect(
        db_type=db_type,
        connection_string=connection_string,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
        query=query,
    )
    session = conn.session()
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


def test_duckdb_connection():
    """Teste la connexion à DuckDB"""
    conn = easydbs.connect(easydbs.DUCKDB)
    session = conn.session()
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


def test_sqlite_connection():
    conn = easydbs.connect(easydbs.SQLITE)
    session = conn.session()
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


@pytest.mark.parametrize(
    "db_url", CONNECTION_STRING.values(), ids=CONNECTION_STRING.keys()
)
def test_connection_with_connection_string(db_url):
    """Teste la connexion aux bases via SQLAlchemy"""
    conn = easydbs.connect(connection_string=db_url)
    session = conn.session()
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


@pytest.mark.skipif(skip_if_arm(), reason="Test not supported on ARM architecture")
def test_connection_mssql():
    conn = easydbs.connect(
        easydbs.MSSQL,
        username="SA",
        password="reallyStrongPwd123",
        host="localhost",
        port=1433,
        query={"driver": "ODBC Driver 18 for SQL Server"},
    )
    session = conn.session()
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)


@pytest.mark.skipif(skip_if_arm(), reason="Test not supported on ARM architecture")
def test_connection_mssql_with_connection_string():
    conn = easydbs.connect(
        connection_string="mssql+pyodbc://SA:reallyStrongPwd123@localhost:1433/?driver=ODBC+Driver+18+for+SQL+Server"
    )
    session = conn.session()
    result = session.exec("SELECT 1").first()
    assert result is not None
    assert result == (1,)
