from .dbapi import ConnectionManager, DBDriver, connect
from .types import *

# Global constants for the database module
SQLITE = DBDriver.SQLITE
MYSQL = DBDriver.MYSQL
POSTGRE = DBDriver.POSTGRE
DUCKDB = DBDriver.DUCKDB
MSSQL = DBDriver.MSSQL
MARIADB = DBDriver.MARIADB

# DB API level supported
apilevel = "2.0"

# Thread safety level
# 0: Threads may not share the module.
# 1: Threads may share the module, but not connections.
# 2: Threads may share the module and connections.
# 3: Threads may share the module, connections, and cursors.
threadsafety = 1

# Parameter style
paramstyle = "qmark"  # Question mark style, e.g., ...WHERE name=?
