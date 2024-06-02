"""
Cursor object for Kinetica which fits the DB API spec.

"""
# pylint: disable=c-extension-no-member
import weakref
from typing import Optional, Sequence, Type, Union, TYPE_CHECKING, List, Iterator

from gpudb import GPUdbSqlIterator, GPUdb
from gpudb.dbapi.core.exceptions import (
    NotSupportedError,
    convert_runtime_errors,
)
from gpudb.dbapi.core.utils import raise_if_closed, ignore_transaction_error
from gpudb.dbapi.pep249 import SQLQuery, QueryParameters, ColumnDescription, ProcName, ProcArgs, ResultRow, ResultSet, \
    CursorConnectionMixin, IterableCursorMixin, TransactionalCursor

if TYPE_CHECKING:
    # pylint: disable=cyclic-import
    from gpudb.dbapi.core import KineticaConnection

__all__ = ["Cursor"]

kinetica_to_python_type_map = {
    'boolean': 'bool',
    'int8': 'int',
    'int16': 'int',
    'int': 'int',
    'long': 'int',
    'float': 'float',
    'double': 'float',
    'decimal': 'decimal.Decimal',
    'string': 'str',
    'char1': 'str',
    'char2': 'str',
    'char4': 'str',
    'char8': 'str',
    'char16': 'str',
    'char32': 'str',
    'char64': 'str',
    'char128': 'str',
    'char256': 'str',
    'ipv4': 'int',
    'uuid': 'uuid.UUID',
    'wkt': 'str',
    "date": 'datetime.date',
    'datetime': 'datetime.datetime',
    'time': 'datetime.time',
    'timestamp': 'int',
    'bytes': 'bytes',
    'wkb': 'str',
}

# pylint: disable=too-many-ancestors
class Cursor(
    CursorConnectionMixin,
    IterableCursorMixin,
    TransactionalCursor
):
    """
    A DB API 2.0 compliant cursor for Kinetica, as outlined in
    PEP 249.

    """

    def __init__(self, connection: "KineticaConnection", query: SQLQuery = None, query_params: QueryParameters = None):
        self._connection: KineticaConnection = weakref.proxy(connection)
        self._sql = query
        self._query_params = query_params
        self._cursors: List[GPUdbSqlIterator] = []
        self.__closed = False

    @classmethod
    def from_connection(cls, conn: "KineticaConnection"):
        return cls(conn)

    @property
    def sql(self):
        return self._sql

    @sql.setter
    def sql(self, value):
        self._sql = value

    @property
    def query_params(self):
        return self._query_params

    @query_params.setter
    def query_params(self, value):
        self._query_params = value

    @property
    def _closed(self) -> bool:
        # pylint: disable=protected-access
        try:
            return self.__closed or self.connection.closed
        except ReferenceError:
            # Parent connection already GC'd.
            return True

    @_closed.setter
    def _closed(self, value: bool):
        self.__closed = value

    @property
    def connection(self) -> "KineticaConnection":
        return self._connection

    @property
    def description(self) -> Optional[Sequence[ColumnDescription]]:
        """
        This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

            name
            type_code
            display_size
            internal_size
            precision
            scale
            null_ok

        The first two items (name and type_code) are mandatory, the other five 
        are optional and are set to None if no meaningful values can be provided.

        This attribute will be None for operations that do not return rows or 
        if the cursor has not had an operation invoked via the .execute*() method yet.

        Returns:
            Optional[Sequence[ColumnDescription]]: a sequence of immutable tuples like:
            [('field_1', <class 'int'>, None, None, None, None, None),
            ('field_2', <class 'int'>, None, None, None, None, None),
            ('field_3', <class 'str'>, None, None, None, None, None),
            ('field_4', <class 'float'>, None, None, None, None, None)]
        """
        try:
            return [(n, eval(kinetica_to_python_type_map[t]), None, None, None, None, None)
                    for n, t in self._cursors[-1].type_map.items()]
        except RuntimeError:
            return None

    @property
    def rowcount(self) -> int:
        return self._cursors[-1].total_count

    @raise_if_closed
    @convert_runtime_errors
    def commit(self) -> None:
        return None

    @raise_if_closed
    @ignore_transaction_error
    @convert_runtime_errors
    def rollback(self) -> None:
        return None

    @convert_runtime_errors
    def close(self) -> None:
        """Close the cursor."""
        if self._closed:
            return
        for cursor in self._cursors:
            cursor.close()
        self._closed = True

    def callproc(
        self, procname: ProcName, parameters: Optional[ProcArgs] = None
    ) -> Optional[ProcArgs]:
        sql_statement = f"EXECUTE PROCEDURE {procname}"
        cursor = self.execute(sql_statement)
        cursor.close()
        return None

    def nextset(self) -> Optional[bool]:
        raise NotSupportedError(
            "Kinetica Cursors do not support more than one result set."
        )

    @raise_if_closed
    def setinputsizes(self, sizes: Sequence[Optional[Union[int, Type]]]) -> None:
        pass

    @raise_if_closed
    def setoutputsize(self, size: int, column: Optional[int]) -> None:
        pass

    @raise_if_closed
    @convert_runtime_errors
    def execute(
        self, operation: SQLQuery, parameters: Optional[QueryParameters] = None
    ) -> "Cursor":
        """ Executes an SQL statement and returns a Cursor instance which can 
            used to iterate over the results of the query

        Args:
            operation (SQLQuery): an SQL statement
            parameters (Optional[QueryParameters], optional): the parameters 
                to the SQL statement; typically a heterogeneous list. Defaults to None.

        Returns:
            Cursor: Returns an instance of self which is used by KineticaConnection
        """
        
        internal_cursor = GPUdbSqlIterator(self._connection.connection,
                                           operation, sql_params=parameters if parameters else [])
        self._cursors.append(internal_cursor)
        self.arraysize = self._cursors[-1].batch_size
        self._cursors[-1].execute(operation)
        self._closed = False
        return self

    def executescript(self, script: SQLQuery) -> "Cursor":
        """Not supported as of now."""
        raise NotSupportedError("Kinetica does not support this call ...")

    @raise_if_closed
    @convert_runtime_errors
    def executemany(
        self, operation: SQLQuery, seq_of_parameters: Sequence[QueryParameters]
    ) -> "Cursor":
        raise NotImplementedError("This method is not implemented ...")

    @raise_if_closed
    @convert_runtime_errors
    def fetchone(self) -> Optional[ResultRow]:
        row = next(self._cursors[-1], None)
        return row

    @raise_if_closed
    @convert_runtime_errors
    def fetchmany(self, size: Optional[int] = None) -> ResultSet:
        if size is None:
            size = self.arraysize
        result_set = []
        for i in range(size):
            row = self.fetchone()
            if row is None:
                break
            result_set.append(row)
        return result_set

    @raise_if_closed
    @convert_runtime_errors
    def fetchall(self) -> ResultSet:
        return self._cursor.fetchall()

    def __iter__(self) -> Union[Iterator[dict], Iterator[tuple]]:
        """Iteration over the result set."""
        while True:
            _next = self.fetchone()
            if _next is None:
                break
            yield _next
