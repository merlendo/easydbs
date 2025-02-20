from __future__ import annotations

import asyncio

from enum import Enum
from typing import Any, Optional, overload
from urllib.parse import parse_qs, urlencode
from sqlmodel import Session, select
import sqlalchemy
from sqlalchemy import Table as _Table
from sqlalchemy.ext.automap import automap_base

from .engine import Engine


class DBDriver(Enum):
    SQLITE = "sqlite"
    MYSQL = "mysql+pymysql"
    POSTGRE = "postgresql+psycopg2"
    DUCKDB = "duckdb"
    MSSQL = "mssql+pyodbc"
    MARIADB = "mysql+pymysql"


def connect(
    db_type: DBDriver,
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
        **kwargs: Any,
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
        else:
            if not drivername:
                raise ValueError(
                    "The parameter 'drivername' is required if 'connection_string' is not set."
                )
            self.drivername = drivername
            self.username = username
            self.password = password
            self.host = host
            self.port = port
            self.database = database
            self.query = query
            self.connection_string = sqlalchemy.URL.create(
                drivername=self.drivername,
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                query=self.query,
            )

        self.engine = sqlalchemy.create_engine(self.connection_string)


class Connection(SQLAlchemyDatabase):
    @overload
    def __init__(self, db_type: str, connection_string: str) -> None: ...

    @overload
    def __init__(
        self,
        db_type: DBDriver,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs: Any,
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
        if connection_string:
            self.id = connection_string
        else:
            self.id = f"{db_type}+{database}"
        super().__init__(
            connection_string=connection_string,
            drivername=db_type.value,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            query=query,
        )

    def __repr__(self):
        return f"<Connection(db_type={self.db_type}, db_name={self.database}, engine={self.engine})>"

    def __call__(self, func):
        """Decorator to manage sessions for both sync and async functions."""
        if asyncio.iscoroutinefunction(func):

            async def async_wrapped(*args, **kwargs):
                cursor = self.cursor()
                print(f"[{self.id}] - Creating cursor for {func.__name__}")
                try:
                    result = await func(cursor, *args, **kwargs)
                finally:
                    cursor.close()
                print(f"[{self.id}] - Closing cursor for {func.__name__}")
                return result

            return async_wrapped
        else:

            def sync_wrapped(*args, **kwargs):
                cursor = self.cursor()
                print(f"[{self.id}] - Creating cursor for {func.__name__}")
                try:
                    result = func(cursor, *args, **kwargs)
                finally:
                    cursor.close()
                print(f"[{self.id}] - Closing cursor for {func.__name__}")
                return result

            return sync_wrapped

    def connect(self):
        """Connects and returns the connection object."""
        return self.raw_connection

    def close(self):
        """Disposes the engine and closes the connection."""
        self.engine.dispose()

    def commit(self):
        """Commits the current transaction."""
        return self.raw_connection.commit()

    def cursor(self) -> Cursor:
        """Returns a Cursor object."""
        return Cursor(self)

    def rollback(self):
        """Rolls back the current transaction."""
        return self.raw_connection.rollback()

    def session(self) -> Session:
        def wrapper(callable):
            def _wrap(q, *args, **kwargs):
                if isinstance(q, str):
                    q = sqlalchemy.text(q)
                return callable(q, *args, **kwargs)

            return _wrap

        session = Session(self.engine)
        session.exec = wrapper(session.exec)
        return session


class Cursor:
    def __init__(self, connection: Connection):
        self.connection = connection
        self.rowcount = self.cursor.rowcount
        self.description = self.cursor.description
        self.session = None

    def __enter__(self):
        self.session = self.connection.sessionmaker()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.commit()
        self.session.close()
        self.close()

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__qualname__} object at {hex(id(self))} @alias {self.cursor.__repr__().strip('<>')}>"

    @property
    def cursor(self):
        with self.connection.engine.connect() as connection:
            raw_connection = connection.connection
            cursor = raw_connection.cursor()
        return cursor

    def callproc(self, procname, **parameters):
        """Not implemented, could execute a stored procedure."""
        return self.cursor.callproc(procname, parameters)

    def close(self):
        return self.cursor.close()

    def execute(self, operation, parameters=[]):
        """Executes a single operation with the provided parameters."""
        return self.cursor.execute(operation, parameters)

    def executemany(self, operation, seq_parameters):
        """Executes the same operation with a sequence of parameters."""
        return self.cursor.executemany(operation, seq_parameters)

    def fetchone(self):
        """Fetches the next row of a query result set."""
        return self.cursor.fetchone()

    def fetchmany(self, size):
        """Fetches the next set of `size` rows of a query result."""
        return self.cursor.fetchmany(size)

    def fetchall(self):
        """Fetches all rows of a query result."""
        return self.cursor.fetchall()

    def nextset(self):
        """Moves to the next result set in a multi-query execution."""
        return self.cursor.nextset()

    def arraysize(self):
        """Gets/sets the number of rows to fetch at once."""
        return self.rowcount

    def setinputsizes(self, sizes):
        """Sets the input sizes for the query parameters."""
        return self.cursor.setinputsizes(sizes)

    def setoutputsize(self, size, column=None):
        """Sets the output size for columns."""
        return self.cursor.setoutputsize(size, column)


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

    def add_connection(self, db_type: DBDriver, **args_connection: Any) -> Connection:
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
