import asyncio

import sqlalchemy
from sqlalchemy.orm import Session

from .engine import Engine


class Connection:
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
        return f"<Connection(engine={self.engine}, db_type={self.db_type}, db_name={self.db_name})>"

    def __call__(self, func):
        if asyncio.iscoroutinefunction(func):
            async def async_wrapped(*args, **kwargs):
                self.session = Session(self.engine)
                print(f"[{self.id}] - Creation Session for {func.__name__}")
                try:
                    result = await func(self.session, *args, **kwargs)
                finally:
                    self.session.close()
                print(f"[{self.id}] - Close Session for {func.__name__}")
                return result
            return async_wrapped
        else:
            def sync_wrapped(*args, **kwargs):
                print(f"[{self.id}] - Creation Session for {func.__name__}")
                self.session = Session(self.engine)
                try:
                    result = func(self.session, *args, **kwargs)
                finally:
                    self.session.close()
                print(f"[{self.id}] - Close Session for {func.__name__}")
                return result
            return sync_wrapped

    def _conn_id(self, db_type: str, db_name: str):
        return f'{db_type}+{db_name}'

    def close(self):
        self.engine.dispose()

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor()


class Cursor():
    def __init__(self):
        self.description = None
        self.rowcount = None

    def callproc(self, procname, **parameters):
        pass

    def close(self):
        pass

    def execute(self, operation, parameter: list):
        pass

    def executemany(self, operation, seq_parameter: list):
        pass

    def fetchone(self):
        pass

    def fetchmany(self, size):
        pass

    def fetchall(self):
        pass

    def nextset(self):
        pass

    @property
    def arraysize(self, size):
        pass

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(size, columns: list):
        pass


class ConnectionManager:
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
                       db_name: str,
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
        for conn in self._connections.values():
            yield conn
