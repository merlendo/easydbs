import asyncio

import sqlalchemy
from sqlalchemy.orm import Session

from .engine import Engine


def connect(db_type: str,
            db_name: str,
            dsn: str | None = None,
            username: str | None = None,
            password: str | None = None,
            host: str | None = None,
            port: int | None = None):
    cm = ConnectionManager()
    return cm.add_connection(db_type=db_type, db_name=db_name, dsn=dsn,
                             username=username, password=password, host=host, port=port)


class Connection:

    session: Session | None

    def __init__(self, db_type: str, db_name: str, dsn: str, username: str, password: str, host: str, port: int):
        self.id = self._conn_id(db_type, db_name)
        self.db_type = db_type
        self.db_name = db_name
        self.dsn = dsn
        self.engine = Engine(db_type, database_name=db_name, dsn=dsn, username=username,
                             password=password, host=host, port=port).create()
        self.session = None
        self.metadata = sqlalchemy.MetaData()
        self.metadata.reflect(bind=self.engine)

    def __repr__(self):
        return f"<Connection(db_type={self.db_type}, db_name={self.db_name}, engine={self.engine})>"

    def __call__(self, func):
        """Decorator to manage sessions for both sync and async functions."""
        if asyncio.iscoroutinefunction(func):
            async def async_wrapped(*args, **kwargs):
                self.session = Session(self.engine)
                print(f"[{self.id}] - Creating session for {func.__name__}")
                try:
                    result = await func(self.session, *args, **kwargs)
                finally:
                    self.session.close()
                print(f"[{self.id}] - Closing session for {func.__name__}")
                return result
            return async_wrapped
        else:
            def sync_wrapped(*args, **kwargs):
                self.session = Session(self.engine)
                print(f"[{self.id}] - Creating session for {func.__name__}")
                try:
                    result = func(self.session, *args, **kwargs)
                finally:
                    self.session.close()
                print(f"[{self.id}] - Closing session for {func.__name__}")
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

    def cursor(self):
        """Returns a Cursor object."""
        return Cursor(self.engine)


class Cursor:
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine
        self.description = None
        self.rowcount = None
        self.cursor = self._cursor()

    def _cursor(self):
        with self.engine.connect() as connection:
            raw_connection = connection.connection
            cursor = raw_connection.cursor()
        return cursor

    def callproc(self, procname, **parameters):
        """Not implemented, could execute a stored procedure."""
        pass

    def close(self):
        self.cursor.close()

    def execute(self, operation, parameters=None):
        """Executes a single operation with the provided parameters."""
        with self.engine.connect() as connection:
            result = connection.execute(operation, parameters)
        return result

    def executemany(self, operation, seq_parameters):
        """Executes the same operation with a sequence of parameters."""
        with self.engine.connect() as connection:
            result = connection.executemany(operation, seq_parameters)
        return result

    def fetchone(self):
        """Fetches the next row of a query result set."""
        pass

    def fetchmany(self, size):
        """Fetches the next set of `size` rows of a query result."""
        pass

    def fetchall(self):
        """Fetches all rows of a query result."""
        pass

    def nextset(self):
        """Moves to the next result set in a multi-query execution."""
        pass

    @property
    def arraysize(self):
        """Gets/sets the number of rows to fetch at once."""
        return self.rowcount

    def setinputsizes(self, sizes):
        """Sets the input sizes for the query parameters."""
        pass

    def setoutputsize(self, size, columns=None):
        """Sets the output size for columns."""
        pass


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

    def add_connection(self, db_type: str, db_name: str, dsn: str, username: str, password: str, host: str, port: int):
        conn = Connection(db_type=db_type, db_name=db_name, dsn=dsn,
                          username=username, password=password, host=host, port=port)
        self._connections[conn.id] = conn
        return self._connections[conn.id]

    def connections(self):
        """Returns all the stored connections."""
        for conn in self._connections.values():
            yield conn


def ConnectionManager():
    return _ConnectionManager()
