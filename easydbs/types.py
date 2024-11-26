import time as py_time  # to avoid conflict with `time` module
from datetime import date, datetime, time


def Date(year, month, day):
    """Constructs an object holding a date value."""
    return date(year, month, day)


def Time(hour, minute, second):
    """Constructs an object holding a time value."""
    return time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    """Constructs an object holding a timestamp value."""
    return datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    """Constructs a date object from the given ticks value."""
    return date.fromtimestamp(ticks)


def TimeFromTicks(ticks):
    """Constructs a time object from the given ticks value."""
    dt = datetime.fromtimestamp(ticks)
    return dt.time()


def TimestampFromTicks(ticks):
    """Constructs a timestamp object from the given ticks value."""
    return datetime.fromtimestamp(ticks)


class Binary:
    """Constructs an object capable of holding a binary (long) string value."""

    def __init__(self, string):
        if not isinstance(string, (bytes, bytearray)):
            raise TypeError("Binary requires a bytes or bytearray object.")
        self.value = string

    def __repr__(self):
        return f"Binary({self.value})"

    def __eq__(self, other):
        return isinstance(other, Binary) and self.value == other.value


class DBType:
    """Base class for type objects."""
    pass


class STRING(DBType):
    """Type for string-based columns."""
    pass


class BINARY(DBType):
    """Type for binary (long) columns."""
    pass


class NUMBER(DBType):
    """Type for numeric columns."""
    pass


class DATETIME(DBType):
    """Type for date/time columns."""
    pass


class ROWID(DBType):
    """Type for row ID columns."""
    pass


# Singletons for type objects
STRING = STRING()
BINARY = BINARY()
NUMBER = NUMBER()
DATETIME = DATETIME()
ROWID = ROWID()
