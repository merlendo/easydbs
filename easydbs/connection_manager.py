import asyncio

from sqlalchemy import MetaData
from sqlalchemy.orm import Session

from .engine import Engine


class Connection:
    def __init__(self, engine, db_type, db_name):
        self.id = self._conn_id(db_type, db_name)
        self.db_type = db_type
        self.db_name = db_name
        self.engine = engine
        self.session = None
        self.metadata = MetaData()
        self.metadata.reflect(bind=engine)

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

    def close():
        pass

    def commit():
        pass

    def rollback():
        pass

    def cursor(self):
        return self.engine.connect()


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
                       username: str | None = None,
                       password: str | None = None,
                       host: str | None = None,
                       port: int | None = None):
        engine = Engine(db_type, database_name=db_name, username=username,
                        password=password, host=host, port=port).create()
        conn = Connection(engine=engine, db_type=db_type, db_name=db_name)
        self._connections[conn.id] = conn
        return self._connections[conn.id]

    def connections(self):
        for conn in self._connections.values():
            yield conn
