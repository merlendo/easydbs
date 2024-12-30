import datetime
import time
from sqlalchemy import Integer, Float, String, DateTime
from typing import Any

# Most of the code is stolen from https://github.com/apache/arrow-adbc/blob/main/python/adbc_driver_manager/adbc_driver_manager/dbapi.py

# Types for the DB-API 2.0 specification
#: The type for date values.
Date = datetime.date
#: The type for time values.
Time = datetime.time
#: The type for timestamp values.
Timestamp = datetime.datetime


def DateFromTicks(ticks: int) -> Date:
    """Construct a date value from a count of seconds."""
    # Standard implementations from PEP 249 itself
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks: int) -> Time:
    """Construct a time value from a count of seconds."""
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks: int) -> Timestamp:
    """Construct a timestamp value from a count of seconds."""
    return Timestamp(*time.localtime(ticks)[:6])


class Binary:
    """Constructs an object capable of holding a binary (long) string value."""

    def __init__(self, string):
        if not isinstance(string, (bytes, bytearray)):
            raise TypeError("Binary requires a bytes or bytearray object.")
        self.value = string

    def __repr__(self):
        return f"Binary({self.value!r})"

    def __eq__(self, other):
        return isinstance(other, Binary) and self.value == other.value
    
class _TypeSet(frozenset):
    """A set of SQLAlchemy type IDs that compares equal to subsets of self."""

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, _TypeSet):
            return not (other - self)
        elif isinstance(other, type):  # ici on vérifie si c'est une classe de type SQLAlchemy
            return other in self
        return False


# PEP 249 type constants
DATETIME = _TypeSet(
    [
        DateTime,  
    ]
)
NUMBER = _TypeSet(
    [
        Integer,  
        Float,    
    ]
)
ROWID = _TypeSet([Integer])  
STRING = _TypeSet([String])  

