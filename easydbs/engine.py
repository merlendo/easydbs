import sqlalchemy


class Engine:
    def __init__(self,
                 db_type: str,
                 database_name: str | None = None,
                 dsn: str | None = None,
                 username: str | None = None,
                 password: str | None = None,
                 host: str | None = None,
                 port: int | None = None):

        self.db_type = db_type
        if dsn:
            self.url_object = sqlalchemy.engine.URL.create(
                drivername=f"{db_type}+pyodbc", query={"odbc_connect": f"DSN={dsn}"}
            )
        else:
            self.url_object = sqlalchemy.URL.create(self.DBAPI,
                                                    username=username,
                                                    password=password,
                                                    host=host,
                                                    port=port,
                                                    database=database_name)

    @property
    def DBAPI(self) -> str:
        match self.db_type:
            case 'sqlite':
                dbapi = "sqlite"
            case 'mysql':
                dbapi = 'mysql+pymysql'
            case 'postgresql':
                dbapi = 'postgresql+psycopg2'
            case 'duckdb':
                dbapi = 'duckdb'
            case 'mssql':
                dbapi = 'mssql+pyodbc'
            case 'mariadb':
                dbapi = 'mysql+pymysql'
            case _:
                dbapi = None
        if not dbapi:
            raise ValueError(f"Database type not suported : {self.db_type}")

        return dbapi

    def create(self) -> sqlalchemy.engine.base.Engine:
        return sqlalchemy.create_engine(self.url_object)
