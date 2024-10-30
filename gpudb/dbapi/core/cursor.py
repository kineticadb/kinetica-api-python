"""
Cursor object for Kinetica which fits the DB API spec.

"""

import json
import re
import datetime, uuid, decimal

# pylint: disable=c-extension-no-member
import weakref
from enum import Enum, unique
from itertools import islice
from typing import (
    Optional,
    Sequence,
    Type,
    Union,
    TYPE_CHECKING,
    List,
    Iterator,
    Tuple,
    Any,
)

from gpudb import GPUdbSqlIterator, GPUdbException
from gpudb.dbapi.core.exceptions import (
    ProgrammingError,
    NotSupportedError,
    convert_runtime_errors,
)
from gpudb.dbapi.core.utils import raise_if_closed, ignore_transaction_error
from gpudb.dbapi.pep249 import (
    SQLQuery,
    QueryParameters,
    ColumnDescription,
    ProcName,
    ProcArgs,
    ResultRow,
    ResultSet,
    CursorConnectionMixin,
    IterableCursorMixin,
    TransactionalCursor,
)

if TYPE_CHECKING:
    # pylint: disable=cyclic-import
    from gpudb.dbapi.core import KineticaConnection


@unique
class ParamStyle(Enum):
    QMARK = "qmark"
    NUMERIC = "numeric"
    FORMAT = "format"
    NUMERIC_DOLLAR = "numeric_dollar"


__all__ = ["Cursor", "ParamStyle"]

kinetica_to_python_type_map = {
    "boolean": "bool",
    "int8": "int",
    "int16": "int",
    "int": "int",
    "long": "int",
    "float": "float",
    "double": "float",
    "decimal": "decimal.Decimal",
    "string": "str",
    "char1": "str",
    "char2": "str",
    "char4": "str",
    "char8": "str",
    "char16": "str",
    "char32": "str",
    "char64": "str",
    "char128": "str",
    "char256": "str",
    "ipv4": "int",
    "uuid": "uuid.UUID",
    "wkt": "str",
    "date": "datetime.date",
    "datetime": "datetime.datetime",
    "time": "datetime.time",
    "timestamp": "int",
    "bytes": "bytes",
    "wkb": "str",
}


# pylint: disable=too-many-ancestors
class Cursor(CursorConnectionMixin, IterableCursorMixin, TransactionalCursor):
    """
    A DB API 2.0 compliant cursor for Kinetica, as outlined in
    PEP 249.

    """

    __INSERT_BATCH_SIZE = 50000

    def __init__(
        self,
        connection: "KineticaConnection",
        query: SQLQuery = None,
        query_params: QueryParameters = None,
    ):
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
        if self._cursors and len(self._cursors) > 0 and self._cursors[-1].type_map:
            try:
                return [
                    (n, eval(kinetica_to_python_type_map[t]), None, None, None, None, None)
                    for n, t in self._cursors[-1].type_map.items()
                ]
            except RuntimeError:
                return None
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
        """Executes an SQL statement and returns a Cursor instance which can
            used to iterate over the results of the query

        Args:
            operation (SQLQuery): an SQL statement
            parameters (Optional[QueryParameters], optional): the parameters
                to the SQL statement; typically a heterogeneous list. Defaults to None.

        Returns:
            Cursor: Returns an instance of self which is used by KineticaConnection
        """

        sql_statement = None
        valid, placeholder = Cursor.__is_valid_statement(operation)
        if valid:
            if placeholder == ParamStyle.QMARK:
                sql_statement = Cursor.__process_params_qmark(operation)
            elif placeholder == ParamStyle.NUMERIC:
                sql_statement = Cursor.__process_params_numeric(operation)
            elif placeholder == ParamStyle.FORMAT:
                sql_statement = Cursor.__process_params_format(operation)
        else:
            raise ProgrammingError(
                "Invalid SQL statement {}; has non-supported parameter placeholders {}".format(
                    operation, placeholder
                )
            )

        sql_statement = sql_statement or operation

        internal_cursor = GPUdbSqlIterator(
            self._connection.connection,
            sql_statement,
            sql_params=list(parameters) if parameters else [],
        )
        self._cursors.append(internal_cursor)
        self.arraysize = self._cursors[-1].batch_size
        self._cursors[-1]._GPUdbSqlIterator__execute(
            sql_statement, parameters=json.dumps(parameters)
        )

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
        def split_list_iter(lst, size):
            it = iter(lst)
            return [list(islice(it, size)) for _ in range(0, len(lst), size)]

        valid, placeholder = Cursor.__is_valid_statement(operation)
        if not valid:
            raise GPUdbException(f"Invalid SQL statement : {operation}")

        statement_type = Cursor.__check_sql_statement_type(operation)

        if statement_type == "INSERT":
            json_lists = split_list_iter(seq_of_parameters, Cursor.__INSERT_BATCH_SIZE)

            for json_list in json_lists:
                resp = self.connection.connection.execute_sql_and_decode(
                    operation, options={"query_parameters": json.dumps(json_list)}
                )
                if resp and resp["status_info"]["status"] == "ERROR":
                    raise GPUdbException(resp["status_info"]["message"])

            return self
        else:
            cursor_list = []
            for params in seq_of_parameters:
                cursor = self.execute(operation, params)
                cursor_list.append(cursor)
            last_cursor = cursor_list[:-1][0]

            for cursor in cursor_list:
                cursor.close()

            return last_cursor

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
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result_set.append(row)
        return result_set

    @raise_if_closed
    @convert_runtime_errors
    def fetchall(self) -> ResultSet:
        return self.fetchmany()

    def __iter__(self) -> Union[Iterator[dict], Iterator[tuple]]:
        """Iteration over the result set."""
        while True:
            _next = self.fetchone()
            if _next is None:
                break
            yield _next

    @staticmethod
    def __has_qmark_params(sql_statement):
        valid, placeholder = Cursor.__is_valid_statement(sql_statement)
        return placeholder == ParamStyle.QMARK and valid

    @staticmethod
    def __has_numeric_params(sql_statement):
        valid, placeholder = Cursor.__is_valid_statement(sql_statement)
        return placeholder == ParamStyle.NUMERIC and valid

    @staticmethod
    def __has_format_params(sql_statement):
        valid, placeholder = Cursor.__is_valid_statement(sql_statement)
        return placeholder == ParamStyle.FORMAT and valid

    @staticmethod
    def __is_valid_statement(
        sql_statement: str,
    ) -> Tuple[Union[bool, Any], Union[str, None]]:
        placeholders = list(
            Cursor.__extract_parameter_placeholders(sql_statement).keys()
        )

        if len(placeholders) > 1:
            raise ProgrammingError(
                f"SQL statement {sql_statement} contains different parameter placeholder formats {placeholders}"
            )

        placeholder = placeholders[0] if len(placeholders) == 1 else None
        supported_paramstyles = [e.value for e in ParamStyle]
        return (
            placeholder is None or placeholder.value in supported_paramstyles,
            placeholder,
        )

    @staticmethod
    def __extract_parameter_placeholders(sql_statement: str):
        # Define regular expression patterns to match different DBAPI v2 placeholders
        patterns = {
            ParamStyle.QMARK: r"\?",  # Question mark style
            ParamStyle.NUMERIC: r":(\d+)",  # Numeric style (e.g., :1, :2)
            ParamStyle.NUMERIC_DOLLAR: r"\$\d+",  # Numeric dollar style (e.g., $1, $2)
            # 'named': r':\w+',  # Named style (e.g., :name)
            ParamStyle.FORMAT: r"%[sdifl]",  # ANSI C printf format codes (e.g., %s)
            # 'pyformat': r'%\(\w+\)s'  # Python extended format codes (e.g., %(name)s)
        }

        # Dictionary to hold found placeholders
        placeholders = {}

        # Extract placeholders based on patterns
        for key, pattern in patterns.items():
            found = re.findall(pattern, sql_statement)
            if len(found) > 0:
                placeholders[key] = found

        return placeholders

    @staticmethod
    def __process_params_qmark(query: str):
        """Replace all occurrences of '?' with $1, $2, ..., $n in the SQL statement

        Args:
            query (str): the SQL statement

        Returns:
            str: the modified SQL statement
        """

        # Initialize a counter to keep track of the occurrences of '?'
        counter = 1

        # Find the last occurrence of '?'
        last_question_mark_index = query.rfind("?")

        # Replace all occurrences of '?' with $1, $2, ..., $n
        replaced_string = ""
        for char in query:
            if char == "?":
                replaced_string += f"${counter}"
                # Increment the counter until the last occurrence of '?'
                if counter < last_question_mark_index + 1:
                    counter += 1
            else:
                replaced_string += char

        return replaced_string

    @staticmethod
    def __process_params_numeric(query: str):
        """Replace each ':n' with corresponding '$n' in the SQL statement

        Args:
            query (str): the SQL statement

        Returns:
            str: the modified SQL statement
        """

        def replacer(match):
            # Check if the match is within a quoted string (single or double quotes)
            if match.group(1):  # If the first group (quoted string) is matched, return it as is
                return match.group(0)
            else:  # Otherwise, replace ':n' with '$n'
                return f"${match.group(2)}"

        # This pattern matches either a quoted string (group 1) or a :n placeholder (group 2)
        pattern = r"(['\"].*?['\"])|:([0-9]+)"

        # Perform the replacement using the pattern and replacer function
        result = re.sub(pattern, replacer, query)
        return result


    @staticmethod
    def __process_params_format(query: str):
        """Replace each ANSI C format specifier with corresponding $n in the SQL statement

        Args:
            query (str): the SQL statement

        Returns:
            str: the modified SQL statement
        """

        # Regular expression pattern to match ANSI C format specifiers
        pattern = r"%[sdifl]"

        # Find all matches
        matches = re.findall(pattern, query)

        # Replace each format specifier with corresponding $n
        for i, match in enumerate(matches, start=1):
            query = query.replace(match, f"${i}", 1)

        return query

    @staticmethod
    def __extract_table_name_from_insert_statement(sql):
        # Define a regex pattern to match the table name in an INSERT statement
        pattern = r"INSERT\s+INTO\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+)"

        # Search for the pattern in the SQL statement
        match = re.search(pattern, sql, re.IGNORECASE)

        # If a match is found, return the table name
        if match:
            return match.group(1)
        else:
            # If no match is found, return None or raise an error
            return None

    @staticmethod
    def __check_sql_statement_type(sql):
        # Trim leading/trailing whitespace and make the statement uppercase for comparison
        sql = sql.strip().upper()

        # Patterns to match INSERT, DELETE, and UPDATE statements
        patterns = {
            "INSERT": r"^INSERT\s+INTO",
            "DELETE": r"^DELETE\s+FROM",
            "UPDATE": r"^UPDATE\s+",
        }

        # Check each pattern against the SQL statement
        for statement_type, pattern in patterns.items():
            if re.match(pattern, sql):
                return statement_type

        # If no match is found, return None
        return None

    @staticmethod
    def classify_sql_statement(sql):
        # Remove leading/trailing whitespace and convert to uppercase
        sql = sql.strip().upper()

        # Check for DDL
        ddl_keywords = ['CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'RENAME']
        if any(sql.startswith(keyword) for keyword in ddl_keywords):
            return 'DDL'

        # Check for DML
        dml_keywords = ['INSERT', 'UPDATE', 'DELETE', 'MERGE']
        if any(sql.startswith(keyword) for keyword in dml_keywords):
            return 'DML'

        # Check for Query (SELECT statement)
        if sql.startswith('SELECT'):
            return 'QUERY'

        # Check for other types if needed (optional)
        other_keywords = ['GRANT', 'REVOKE', 'CALL', 'EXPLAIN']
        if any(sql.startswith(keyword) for keyword in other_keywords):
            return 'OTHER'

        # If no matches found
        return 'UNKNOWN'
