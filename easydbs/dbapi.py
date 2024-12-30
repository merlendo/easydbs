from __future__ import annotations

import asyncio

import sqlalchemy
from sqlalchemy import Table as _Table
from sqlalchemy.orm import Session, DeclarativeBase
from sqlalchemy.ext.declarative import declarative_base


from .engine import Engine


def connect(db_type: str,
            db_name: str,
            dsn: str | None = None,
            username: str | None = None,
            password: str | None = None,
            host: str | None = None,
            port: int | None = None) -> Connection:
    cm = ConnectionManager()
    return cm.add_connection(db_type=db_type, db_name=db_name, dsn=dsn,
                             username=username, password=password, host=host, port=port)

class Connection:

    session: Session | None
    metadata: sqlalchemy.MetaData
    tables: dict
    base: DeclarativeBase

    def __init__(self, db_type: str, db_name: str, dsn: str, username: str, password: str, host: str, port: int):
        self.id = self._conn_id(db_type, db_name)
        self.db_type = db_type
        self.db_name = db_name
        self.dsn = dsn
        self.engine = Engine(db_type, db_name=db_name, dsn=dsn, username=username,
                             password=password, host=host, port=port).create()
        self.session = None
        self.metadata = sqlalchemy.MetaData()
        self.metadata.reflect(bind=self.engine)
        self.tables = self.metadata.tables
        self.base = DeclarativeBase()

    def __repr__(self):
        return f"<Connection(db_type={self.db_type}, db_name={self.db_name}, engine={self.engine})>"

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

    def _conn_id(self, db_type: str, db_name: str):
        return f'{db_type}+{db_name}'

    def connect(self):
        """Connects and returns the connection object."""
        return self.engine.connect()

    def close(self):
        """Disposes the engine and closes the connection."""
        self.engine.dispose()

    def commit(self):
        """Commits the current transaction."""
        if self.session:
            self.session.commit()

    def rollback(self):
        """Rolls back the current transaction."""
        if self.session:
            self.session.rollback()

    def cursor(self) -> Cursor:
        """Returns a Cursor object."""
        return Cursor(self.engine)

    def delete(self, whereclause=None, **kwargs):
        return sqlalchemy.delete(self, whereclause, **kwargs)
    
    def create_table(self, name, *args, **kwargs):
        return Table(name, self, *args, **kwargs)

class Cursor:
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine
        self.cursor = self._cursor()
        self.rowcount = self.cursor.rowcount
        self.description = self.cursor.description

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__qualname__} object at {hex(id(self))} @alias {self.cursor.__repr__().strip('<>')}>"

    def _cursor(self):
        with self.engine.connect() as connection:
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


class Table(_Table):
    def __init__(self, name, connection: Connection, *args, **kwargs):
        super().__init__(name, connection.metadata, *args, **kwargs)
        self.connection = connection
        connection.metadata.tables[name] = self

    def create(self):
        """Crée la table dans la base de données."""
        self.create(bind=self.connection.engine)

    def drop(self):
        """Supprime la table de la base de données."""
        self.drop(bind=self.connection.engine)


class _ConnectionManager:
    _instance = None
    _connections: dict

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
            cls._instance._connections = {}
        return cls._instance

    def __init__(self):
        pass

    def add_connection(self,
                       db_type: str,
                       db_name: str | None = None,
                       dsn: str | None = None,
                       username: str | None = None,
                       password: str | None = None,
                       host: str | None = None,
                       port: int | None = None):
        conn = Connection(db_type=db_type, db_name=db_name, dsn=dsn,
                          username=username, password=password, host=host, port=port)
        self._connections[conn.id] = conn
        return self._connections[conn.id]

    def connections(self):
        """Returns all the stored connections."""
        for conn in self._connections.values():
            yield conn


ConnectionManager = _ConnectionManager
