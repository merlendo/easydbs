from .config_manager import ConfigManager
from .connection import ConnectionManager

# Global constants for the database module

# DB API level supported
apilevel = "2.0"

# Thread safety level
# 0: Threads may not share the module.
# 1: Threads may share the module, but not connections.
# 2: Threads may share the module and connections.
# 3: Threads may share the module, connections, and cursors.
threadsafety = 0

# Parameter style
paramstyle = 'qmark'  # Question mark style, e.g., ...WHERE name=?
