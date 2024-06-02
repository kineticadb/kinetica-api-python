"""
Example type constructors as specified by PEP 249, returning the
appropriate Python types. For a concrete implementation, these might
return something more database specific.

"""
import datetime as dt

__all__ = [
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
]

# Constructor definitions.
Date = dt.date
Time = dt.time
Timestamp = dt.datetime
DateFromTicks = dt.date.fromtimestamp
TimestampFromTicks = dt.datetime.fromtimestamp


def TimeFromTicks(timestamp: float):  # pylint: disable=invalid-name
    """Return the time, given a Unix timestamp."""
    return dt.datetime.fromtimestamp(timestamp).time()


Binary = bytes

# Type definitions.
STRING = str
BINARY = bytes
NUMBER = float
DATETIME = dt.datetime
ROWID = str
