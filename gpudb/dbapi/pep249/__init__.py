"""
An abstract implementation of the DB-2.0 API, as outlined in PEP-249.

This package contains abstract base classes which should help with
building conformant database APIs. Inheriting from these base classes
will enforce implementation of the relevant functions, and will implement
some functionality (e.g. context managers) for free.

"""
from gpudb.dbapi.pep249.connection import Connection, TransactionlessConnection
from gpudb.dbapi.pep249.cursor import Cursor, TransactionalCursor, CursorExecuteMixin, CursorFetchMixin
from gpudb.dbapi.pep249.transactions import (
    TransactionFreeContextMixin,
    TransactionContextMixin,
    DummyTransactionContextMixin,
)
from gpudb.dbapi.pep249.exceptions import *
from gpudb.dbapi.pep249.extensions import *
from gpudb.dbapi.pep249.types import *

__version__ = "0.0.1b3"

__all__ = [
    "Connection",
    "TransactionlessConnection",
    "Cursor",
    "TransactionalCursor",
    "CursorExecuteMixin",
    "CursorFetchMixin",
    "ConnectionErrorsMixin",
    "CursorConnectionMixin",
    "IterableCursorMixin",
    "TransactionFreeContextMixin",
    "TransactionContextMixin",
    "DummyTransactionContextMixin",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "ConcreteErrorMixin",
    "SQLQuery",
    "QueryParameters",
    "ResultRow",
    "ResultSet",
    "ColumnDescription",
    "ProcName",
    "ProcArgs",
]
