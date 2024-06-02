"""
Type aliases useful for the abstract DB API implementations.

"""
from typing import Any, Dict, Optional, Sequence, Tuple, Union

__all__ = [
    "SQLQuery",
    "QueryParameters",
    "ResultRow",
    "ResultSet",
    "ColumnDescription",
    "ProcName",
    "ProcArgs",
]

# An SQL query/command, e.g. a complete script or a parameterised
# statement.
SQLQuery = str

# Parameters to be passed to the query.
# https://www.python.org/dev/peps/pep-0249/#paramstyle
# For the typical 'qmark' style, these will typically be passed as a
# sequence of types (although this could be a dict with integer keys).
# For other styles, parameters can be passed as a dict of string to
# other types.
QueryParameters = Union[Sequence[Any], Dict[Union[str, int], Any]]

# A row of returned types. Typically a tuple of Python types which match
# the database column types, sometimes a dict of column name to value.
ResultRow = Union[Sequence[Any], Dict[str, Any]]
# A sequence of result rows - the full set or part of a set.
ResultSet = Sequence[ResultRow]

# The description attributes.
# https://www.python.org/dev/peps/pep-0249/#description
# There isn't a particularly good reference for these, it is
# common to supply only the first two.
Name = str
TypeCode = type
DisplaySize = int
InternalSize = int
Precision = int
Scale = int
NullOK = bool
# The full description.
ColumnDescription = Tuple[
    Name,
    TypeCode,
    Optional[DisplaySize],
    Optional[InternalSize],
    Optional[Precision],
    Optional[Scale],
    Optional[NullOK],
]

# Used with Cursor.callproc.
ProcName = str
ProcArgs = Sequence[Any]
