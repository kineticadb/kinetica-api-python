"""
An abstract database cursor implementation, conformant with PEP 249.

Because it is common for database connections to implement the execute
functionality of the cursor, returning a cursor containing the results,
this is implemented using a set of mixins:

 - CursorExecuteMixin
 - CursorFechMixin
 - CursorSetSizeMixin

"""
from abc import ABCMeta, abstractmethod
from typing import Optional, Sequence, Type, TypeVar, Union
from gpudb.dbapi.pep249.transactions import TransactionFreeContextMixin, TransactionContextMixin
from gpudb.dbapi.pep249.types import (
    QueryParameters,
    ResultRow,
    ResultSet,
    SQLQuery,
    ColumnDescription,
    ProcName,
    ProcArgs,
)


CursorType = TypeVar("CursorType", "Cursor", "TransactionalCursor")


class CursorExecuteMixin(metaclass=ABCMeta):
    """
    The execute portions of a PEP 249 compliant Cursor protocol.

    This could also be used to implement 'execute' support within the
    database connection, as SQLite does.

    """

    @abstractmethod
    def execute(
        self: CursorType,
        operation: SQLQuery,
        parameters: Optional[QueryParameters] = None,
    ) -> CursorType:
        """
        Execute an SQL query. Values may be bound by passing parameters
        as outlined in PEP 249.

        """
        raise NotImplementedError

    @abstractmethod
    def executemany(
        self: CursorType,
        operation: SQLQuery,
        seq_of_parameters: Sequence[QueryParameters],
    ) -> CursorType:
        """
        Execute an SQL query, parameterising the query with sequences
        or mappings passed as parameters.

        """
        raise NotImplementedError

    @abstractmethod
    def callproc(
        self: CursorType, procname: ProcName, parameters: Optional[ProcArgs] = None
    ) -> Optional[ProcArgs]:
        """
        Execute an SQL stored procedure, passing the sequence of parameters.
        The parameters should contain one entry for each procedure argument.

        The result of the call is returned as a modified copy of the input
        parameters. The procedure may also provide a result set, which
        can be made available through the standard fetch methods.

        """
        raise NotImplementedError


class CursorSetSizeMixin(metaclass=ABCMeta):
    """An implementation of size setting for cursor."""

    @abstractmethod
    def setinputsizes(
        self: CursorType, sizes: Sequence[Optional[Union[int, Type]]]
    ) -> None:
        """
        Can be used before a call to `execute` to predefine memory areas
        for the operation's parameters.

        `sizes` is a sequence containing an item - a type, or an integer
        specifying the maximum length for a string - for each input
        parameter. If the item is None, no memory will be reserved for
        that column.

        Implementations are free to have this method do nothing.
        """
        raise NotImplementedError

    @abstractmethod
    def setoutputsize(self: CursorType, size: int, column: Optional[int]) -> None:
        """
        Can be used before a call to `execute` to predefine buffer
        sizes for fetches of 'large' columns (e.g. LONG, BLOB, etc.).

        `size` is an int, referring to the size of the column.

        `column` is an optional int, referring to the index in the
        result sequence. If this is not provided, this will set
        the default size for all 'large' columns in the cursor.

        Implementations are free to have this method do nothing.
        """
        raise NotImplementedError


class CursorFetchMixin(metaclass=ABCMeta):
    """The fetch portions of a PEP 249 compliant Cursor protocol."""

    @property
    @abstractmethod
    def description(self: CursorType) -> Optional[Sequence[ColumnDescription]]:
        """
        A read-only attribute returning a sequence containing a description
        (a seven-item sequence) for each column in the result set.

        The values returned for each column are outlined in the PEP:
        https://www.python.org/dev/peps/pep-0249/#description

        If there is no result set, return None.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def rowcount(self: CursorType) -> int:
        """
        A read-only attribute returning the number of rows that the last
        execute call returned (for e.g. SELECT calls) or affected (for e.g.
        UPDATE/INSERT calls).

        If no execute has been performed or the rowcount cannot be determined,
        this should return -1.
        """
        raise NotImplementedError

    @property
    def arraysize(self: CursorType) -> int:
        """
        An attribute specifying the number of rows to fetch at a time with
        `fetchmany`.

        Defaults to 1, meaning fetch a single row at a time.
        """
        return getattr(self, "_arraysize", 1)

    @arraysize.setter
    def arraysize(self: CursorType, value: int):
        setattr(self, "_arraysize", value)

    @abstractmethod
    def fetchone(self: CursorType) -> Optional[ResultRow]:
        """
        Fetch the next row from the query result set as a sequence of Python
        types (or return None when no more rows are available).

        If the previous call to `execute` did not produce a result set, an
        error can be raised.

        """
        raise NotImplementedError

    def fetchmany(self: CursorType, size: Optional[int] = None) -> ResultSet:
        """
        Fetch the next `size` rows from the query result set as a list
        of sequences of Python types.

        If the size parameter is not supplied, the arraysize property will
        be used instead.

        If rows in the result set have been exhausted, an empty list
        will be returned. If the previous call to `execute` did not
        produce a result set, an error can be raised.

        """
        raise NotImplementedError

    @abstractmethod
    def fetchall(self: CursorType) -> ResultSet:
        """
        Fetch the remaining rows from the query result set as a list of
        sequences of Python types.

        If rows in the result set have been exhausted, an empty list
        will be returned. If the previous call to `execute` did not
        produce a result set, an error can be raised.

        """
        raise NotImplementedError

    @abstractmethod
    def nextset(self: CursorType) -> Optional[bool]:
        """
        Skip the cursor to the next available result set, discarding
        rows from the current set. If there are no more sets, return
        None, otherwise return True.

        This method is optional, as not all databases implement multiple
        result sets.
        """
        raise NotImplementedError


class BaseCursor(
    CursorFetchMixin, CursorExecuteMixin, CursorSetSizeMixin, metaclass=ABCMeta
):
    """A Cursor without an associated context."""


class Cursor(TransactionFreeContextMixin, BaseCursor, metaclass=ABCMeta):
    """A PEP 249 compliant Cursor protocol."""


class TransactionalCursor(
    TransactionContextMixin,
    BaseCursor,
    metaclass=ABCMeta,
):
    """
    A slightly non-compliant Cursor for a database which implements
    transactions on a per-cursor level.

    """
