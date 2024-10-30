"""
An abstract database cursor implementation, conformant with PEP 249.

Because it is common for database connections to implement the execute
functionality of the cursor, returning a cursor containing the results,
this is implemented using a set of mixins:

 - AsyncCursorExecuteMixin
 - AsyncCursorFechMixin
 - CursorSetSizeMixin

"""
from abc import ABCMeta, abstractmethod
from typing import Optional, Sequence, TypeVar

from gpudb.dbapi.pep249.aiopep249.transactions import AsyncTransactionFreeContextMixin, AsyncTransactionContextMixin
from gpudb.dbapi.pep249.aiopep249.types import (
    QueryParameters,
    ResultRow,
    ResultSet,
    SQLQuery,
    ColumnDescription,
    ProcName,
    ProcArgs,
)
from gpudb.dbapi.pep249.cursor import CursorSetSizeMixin


AsyncCursorType = TypeVar(
    "AsyncCursorType",
    "AsyncCursor",
    "TransactionalAsyncCursor",
)


class AsyncCursorExecuteMixin(metaclass=ABCMeta):
    """
    The execute portions of a PEP 249 compliant Cursor protocol.

    This could also be used to implement 'execute' support within the
    database connection, as SQLite does.

    """

    @abstractmethod
    async def execute(
        self: AsyncCursorType,
        operation: SQLQuery,
        parameters: Optional[QueryParameters] = None,
    ) -> AsyncCursorType:
        """
        Execute an SQL query. Values may be bound by passing parameters
        as outlined in PEP 249.

        """
        raise NotImplementedError

    @abstractmethod
    async def executemany(
        self: AsyncCursorType,
        operation: SQLQuery,
        seq_of_parameters: Sequence[QueryParameters],
    ) -> AsyncCursorType:
        """
        Execute an SQL query, parameterising the query with sequences
        or mappings passed as parameters.

        """
        raise NotImplementedError

    @abstractmethod
    async def callproc(
        self: AsyncCursorType, procname: ProcName, parameters: Optional[ProcArgs] = None
    ) -> Optional[ProcArgs]:
        """
        Execute an SQL stored procedure, passing the sequence of parameters.
        The parameters should contain one entry for each procedure argument.

        The result of the call is returned as a modified copy of the input
        parameters. The procedure may also provide a result set, which
        can be made available through the standard fetch methods.

        """
        raise NotImplementedError


class AsyncCursorFetchMixin(metaclass=ABCMeta):
    """The fetch portions of a PEP 249 compliant Cursor protocol."""

    @property
    @abstractmethod
    def description(self: AsyncCursorType) -> Optional[Sequence[ColumnDescription]]:
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
    def rowcount(self: AsyncCursorType) -> int:
        """
        A read-only attribute returning the number of rows that the last
        execute call returned (for e.g. SELECT calls) or affected (for e.g.
        UPDATE/INSERT calls).

        If no execute has been performed or the rowcount cannot be determined,
        this should return -1.
        """
        raise NotImplementedError

    @property
    def arraysize(self: AsyncCursorType) -> int:
        """
        An attribute specifying the number of rows to fetch at a time with
        `fetchmany`.

        Defaults to 1, meaning fetch a single row at a time.
        """
        return getattr(self, "_arraysize", 1)

    @arraysize.setter
    def arraysize(self: AsyncCursorType, value: int):
        setattr(self, "_arraysize", value)

    @abstractmethod
    async def fetchone(self: AsyncCursorType) -> Optional[ResultRow]:
        """
        Fetch the next row from the query result set as a sequence of Python
        types (or return None when no more rows are available).

        If the previous call to `execute` did not produce a result set, an
        error can be raised.

        """
        raise NotImplementedError

    async def fetchmany(self: AsyncCursorType, size: Optional[int] = None) -> ResultSet:
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
    async def fetchall(self: AsyncCursorType) -> ResultSet:
        """
        Fetch the remaining rows from the query result set as a list of
        sequences of Python types.

        If rows in the result set have been exhausted, an empty list
        will be returned. If the previous call to `execute` did not
        produce a result set, an error can be raised.

        """
        raise NotImplementedError

    @abstractmethod
    async def nextset(self: AsyncCursorType) -> Optional[bool]:
        """
        Skip the cursor to the next available result set, discarding
        rows from the current set. If there are no more sets, return
        None, otherwise return True.

        This method is optional, as not all databases implement multiple
        result sets.
        """
        raise NotImplementedError


class BaseAsyncCursor(
    AsyncCursorFetchMixin,
    AsyncCursorExecuteMixin,
    CursorSetSizeMixin,
    metaclass=ABCMeta,
):
    """A Cursor without an associated context."""


class AsyncCursor(AsyncTransactionFreeContextMixin, BaseAsyncCursor, metaclass=ABCMeta):
    """A PEP 249 compliant Cursor protocol."""


class TransactionalAsyncCursor(
    AsyncTransactionContextMixin,
    BaseAsyncCursor,
    metaclass=ABCMeta,
):
    """
    A slightly non-compliant Cursor for a database which implements
    transactions on a per-cursor level.

    """
