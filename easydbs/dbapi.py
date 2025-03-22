from __future__ import annotations

import asyncio
from enum import Enum
from typing import Any, Optional, overload

import sqlalchemy
import sqlmodel
from sqlmodel import Session, SQLModel


class DBDriver(Enum):
    SQLITE = "sqlite"
    MYSQL = "mysql+pymysql"
    POSTGRE = "postgresql+psycopg2"
    DUCKDB = "duckdb"
    MSSQL = "mssql+pyodbc"
    MARIADB = "mysql+pymysql"


def connect(
    db_type: Optional[DBDriver] = None,
    connection_string: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[str] = None,
    database: Optional[str] = None,
    query: Optional[dict] = None,
) -> Connection:
    cm = ConnectionManager()
    return cm.add_connection(
        db_type=db_type,
        connection_string=connection_string,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
        query=query,
    )


class SQLAlchemyDatabase:
    connection_string: sqlalchemy.engine.url.URL

    @overload
    def __init__(self, connection_string: str): ...

    @overload
    def __init__(
        self,
        drivername: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None,
        query: Optional[dict] = None
    ): ...

    def __init__(
        self,
        connection_string: Optional[str] = None,
        drivername: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None,
        query: Optional[dict] = None,
    ):
        """
        Init SQLAlchemyDatabase. Can take a complete connection url, or argument
        to build the url.
        """
        if connection_string:
            self.connection_string = sqlalchemy.engine.url.make_url(connection_string)
            self.engine = sqlmodel.create_engine(self.connection_string)
            self.drivername = drivername
            self.username = self.engine.url.username
            self.password = self.engine.url.password
            self.host = self.engine.url.host
            self.port = self.engine.url.port
            self.database = self.engine.url.database
            self.query = self.engine.url.query
        else:
            drivername, username, password, host, port, database, query = (
                self._check_params_connection(
                    drivername, username, password, host, port, database, query
                )
            )
            self.drivername = drivername
            self.username = username
            self.password = password
            self.host = host
            self.port = port
            self.database = database
            self.query = query
            self.connection_string = sqlalchemy.URL.create(
                drivername=self.drivername.value,
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                query=self.query,
            )
            self.engine = sqlmodel.create_engine(self.connection_string)

    def _check_params_connection(
        self,
        drivername: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None,
        query: Optional[dict] = None,
    ):
        if not drivername:
            raise ValueError(
                "The parameter 'drivername' is required if 'connection_string' is not set."
            )
        if drivername == DBDriver.SQLITE:
            password = None
            host = None
            query = None
        elif drivername == DBDriver.MSSQL:
            if not query:
                raise ValueError(
                    "The version of the ODBC driver is required for SqlServer connection. (e.g. {'driver': 'ODBC Driver 18 for SQL Server'})"
                )
        return drivername, username, password, host, port, database, query


class Connection(SQLAlchemyDatabase):
    @overload
    def __init__(self, connection_string: str) -> None: ...

    @overload
    def __init__(
        self,
        db_type: DBDriver,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None,
        query: Optional[dict] = None,
    ): ...

    def __init__(
        self,
        db_type: DBDriver,
        connection_string: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None,
        query: Optional[dict] = None,
    ):
        self.db_type = db_type
        super().__init__(
            connection_string=connection_string,
            drivername=db_type,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            query=query,
        )
        self._raw_connection = self.engine.raw_connection()
        self.id = f"{self.connection_string.get_backend_name()}+{self.connection_string.database}"

    def __repr__(self):
        return f"<Connection(db_type={self.db_type}, db_name={self.database}, engine={self.engine})>"

    def __call__(self, func):
        """Decorator to manage sessions for both sync and async functions."""
        if asyncio.iscoroutinefunction(func):

            async def async_wrapped(*args, **kwargs):
                session = self.session()
                try:
                    result = await func(session, *args, **kwargs)
                finally:
                    session.close()
                return result

            return async_wrapped

        else:

            def sync_wrapped(*args, **kwargs):
                session = self.session()
                try:
                    result = func(session, *args, **kwargs)
                finally:
                    session.close()
                return result

            return sync_wrapped

    def connect(self):
        """Connects and returns the connection object."""
        return self._raw_connection

    def close(self):
        """Disposes the engine and closes the connection."""
        self.engine.dispose()

    def commit(self):
        """Commits the current transaction."""
        return self._raw_connection.commit()

    def cursor(self):
        """Returns a Cursor object."""
        return self._raw_connection.cursor()

    def rollback(self):
        """Rolls back the current transaction."""
        return self._raw_connection.rollback()

    def session(self) -> Session:
        """Return a sqlmodel (or sqlalchemy) session."""

        def wrapper(callable):
            """Wrap the exec function to use text for sql query."""

            def _wrap(q, *args, **kwargs):
                if isinstance(q, str):
                    q = sqlalchemy.text(q)
                return callable(q, *args, **kwargs)

            return _wrap

        session = Session(self.engine)
        session.exec = wrapper(session.exec)
        return session

    def create_tables(self, tables_names: list[str] | None = None):
        """Create a list of tables of all tables in the SQLModel."""
        if tables_names:
            tables = [
                SQLModel.metadata.tables.get(table)
                for table in tables_names
                if SQLModel.metadata.tables.get(table) is not None
            ]
            SQLModel.metadata.create_all(self.engine, tables=tables)
        else:
            SQLModel.metadata.create_all(self.engine)



class ConnectionManager:
    _instance = None
    _connections: dict[str, Connection]

    def __new__(cls, *args, **kwargs):
        """
        Singleton instance creation for ConnectionManager.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
            cls._instance._connections = {}
        return cls._instance

    def __init__(self):
        """
        Initialize ConnectionManager.
        """
        pass

    def __getitem__(self, index) -> Connection | None:
        try:
            return self._connections[index]
        except KeyError:
            print(f"No connection for the index '{index}'.")
            return None

    def add_connection(
        self, db_type: Optional[DBDriver] = None, **args_connection: Any
    ) -> Connection:
        """Add connection to to the connection manager."""
        conn = Connection(db_type, **args_connection)
        self._connections[conn.id] = conn
        return self._connections[conn.id]

    def connections(self):
        """Yield the stored connections."""
        for conn in self._connections.values():
            yield conn

    def close(self, name: str, *args, **kwargs):
        """
        Close the connection to the specified database.
        """
        self[name].close(*args, **kwargs)
        del self._connections[name]

    def closeall(self):
        """
        Close all open connections.
        """
        for name in self.connections():
            self.close(name)
