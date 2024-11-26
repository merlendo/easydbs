class Warning(Exception):
    """Exception raised for important warnings like data truncations, etc."""
    pass


class Error(Exception):
    """Base class for all error exceptions."""
    pass


class InterfaceError(Error):
    """Exception for errors related to the database interface."""
    pass


class DatabaseError(Error):
    """Exception for errors related to the database itself."""
    pass


class DataError(DatabaseError):
    """Exception for errors due to problems with the processed data."""
    pass


class OperationalError(DatabaseError):
    """Exception for errors related to the database's operation."""
    pass


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected."""
    pass


class InternalError(DatabaseError):
    """Exception for internal database errors."""
    pass


class ProgrammingError(DatabaseError):
    """Exception for programming errors."""
    pass


class NotSupportedError(DatabaseError):
    """Exception for unsupported operations by the database."""
    pass
