"""
Connection object for Kinetica which fits the DB API spec.

"""
import re
from enum import Enum, unique
# pylint: disable=c-extension-no-member
from typing import Dict, Optional, Sequence, Tuple, Union, Any

from gpudb import GPUdb
from gpudb.dbapi.core.cursor import Cursor
from gpudb.dbapi.core.exceptions import convert_runtime_errors, ProgrammingError
from gpudb.dbapi.core.utils import raise_if_closed, ignore_transaction_error
from gpudb.dbapi.pep249 import (SQLQuery,
                                QueryParameters,
                                ProcName,
                                ProcArgs,
                                CursorExecuteMixin,
                                ConcreteErrorMixin,
                                Connection)
from gpudb.dbapi import *

__all__ = ["KineticaConnection", "ParamStyle"]


@unique
class ParamStyle(Enum):
    QMARK = "qmark"
    NUMERIC = "numeric"
    FORMAT = "format"
    NUMERIC_DOLLAR = "numeric_dollar"


DEFAULT_CONFIGURATION: Dict[str, Tuple[Any, Union[type, Tuple[type, ...]]]] = {
    "paramstyle": (None, (type(None), str)),  # standard/kinetica/qmark
}


# pylint: disable=too-many-ancestors
class KineticaConnection(
    CursorExecuteMixin, ConcreteErrorMixin, Connection
):
    """
    A DB API 2.0 compliant connection for Kinetica, as outlined in
    PEP 249.

    """

    def __init__(
            self,
            param_style: Optional[str] = ParamStyle.NUMERIC_DOLLAR,
            *,
            url: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            connection_options: Optional[dict] = None,
    ):
        """ Constructor
        Called by :py:meth:`connect()` in :py:mod:`gpudb.dbapi`

        Args:
            param_style (Optional[str], optional): String constant stating the type of parameter marker formatting
                expected by the interface. Defaults to ParamStyle.NUMERIC_DOLLAR.
            url (Optional[str], optional): the Kinetica URL; has to be keyword only. Defaults to None.
            username (Optional[str], optional): the Kinetica username; has to be keyword only. Defaults to None.
            password (Optional[str], optional): the Kinetica password; has to be keyword only. Defaults to None.
            connection_options (Optional[dict], optional): Defaults to None.

        Raises:
            ProgrammingError: Raised in case of incorrect parameters passed in
        """
        self._closed = False
        self._param_style = param_style

        if not url or not len(url) > 0:
            raise ProgrammingError("Server url must be given ...")
        options = GPUdb.Options()
        if username and len(username) > 0 and password:
            options.username = username
            options.password = password
        if connection_options and 'bypass_ssl_cert_check' in connection_options:
            options.skip_ssl_cert_verification = connection_options['bypass_ssl_cert_check']
        self._connection = GPUdb(host=url, options=options)

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
        """Close the database connection."""
        if self._closed:
            return

        self._closed = True

    @raise_if_closed
    @convert_runtime_errors
    def cursor(self) -> Cursor:
        return Cursor(self)

    def callproc(
            self, procname: ProcName, parameters: Optional[ProcArgs] = None
    ) -> Optional[ProcArgs]:
        """ This method is used to call a Kinetica procedure
            Call a stored database procedure with the given name. The sequence of parameters must
            contain one entry for each argument that the procedure expects.
            The result of the call is returned as modified copy of the input sequence.
            Input parameters are left untouched, output and input/output parameters replaced with possibly new values.

            The procedure may also provide a result set as output.

        Args:
            procname (ProcName): the name of the procedure
            parameters (Optional[ProcArgs], optional): the parameters to the procedure, which Kinetica doesn't support
            as of now. Defaults to None. This is ignored for now

        Returns:
            Optional[ProcArgs]: None
        """
        return self.cursor().callproc(procname, parameters)

    def execute(
            self, sql_statement: SQLQuery, parameters: Optional[QueryParameters] = None
    ) -> Cursor:
        """ The method to execute a single SQL statement (query or command) and return a cursor

        Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
        Variables are specified in a database-specific notation (see the module’s paramstyle attribute for details).

        .. seealso:: :obj:`ParamStyle`

        Args:
            sql_statement (SQLQuery): the SQL statement (query or command) to execute
            parameters (Optional[QueryParameters], optional): the parameters to the query;
                typically a heterogeneous list. Defaults to None.

        Raises:
            ProgrammingError: Raised in case the SQL statement passed in is invalid

        Returns:
            Cursor: a Cursor containing the results of the query
        """
        valid, placeholder = KineticaConnection.__is_valid_statement(sql_statement)
        if valid:
            if placeholder == ParamStyle.QMARK:
                sql_statement = KineticaConnection.__process_params_qmark(sql_statement)
            elif placeholder == ParamStyle.NUMERIC:
                sql_statement = KineticaConnection.__process_params_numeric(sql_statement)
            elif placeholder == ParamStyle.FORMAT:
                sql_statement = KineticaConnection.__process_params_format(sql_statement)
        else:
            raise ProgrammingError("Invalid SQL statement {}; has non-supported parameter placeholders {}"
                                   .format(sql_statement, placeholder))
        return self.cursor().execute(sql_statement, parameters)

    def executemany(
            self, operation: SQLQuery, seq_of_parameters: Sequence[QueryParameters]
    ) -> Cursor:
        """ Method used to execute the same statement with a sequence of parameter values.
            The cursor is only returned from the last execution

        .. seealso:: :func:`execute`

        Args:
            operation (SQLQuery): the SQL query
            seq_of_parameters (Sequence[QueryParameters]): a list of parameters (tuples)

        Returns:
            Cursor: a Cursor instance to iterate over the results
        """
        cursor_list = []
        for params in seq_of_parameters:
            cursor = self.execute(operation, params)
            cursor_list.append(cursor)
        last_cursor = cursor_list[:-1][0]

        for cursor in cursor_list:
            cursor.close()

        return last_cursor

    def executescript(self, script: SQLQuery) -> Cursor:
        """ This method executes an SQL script which is a ';' separated list of SQL statements.

        .. seealso:: :func:`execute`

        Args:
            script (SQLQuery): an SQL script

        Returns:
            Cursor: the Cursor returned as a result of execution of the last statement in the script
        """
        import sqlparse
        sql_statements = sqlparse.split(script)
        last_cursor = None

        if sql_statements and len(sql_statements) > 0:
            cursor_list = []
            for sql_statement in sql_statements:
                cursor = self.execute(sql_statement)
                cursor_list.append(cursor)
            last_cursor = cursor_list[:-1][0]

            for cursor in cursor_list:
                cursor.close()

        return last_cursor

    @property
    def connection(self):
        return self._connection

    @property
    def closed(self):
        return self._closed

    @staticmethod
    def __has_qmark_params(sql_statement):
        valid, placeholder = KineticaConnection.__is_valid_statement(sql_statement)
        return placeholder == ParamStyle.QMARK and valid

    @staticmethod
    def __has_numeric_params(sql_statement):
        valid, placeholder = KineticaConnection.__is_valid_statement(sql_statement)
        return placeholder == ParamStyle.NUMERIC and valid

    @staticmethod
    def __has_format_params(sql_statement):
        valid, placeholder = KineticaConnection.__is_valid_statement(sql_statement)
        return placeholder == ParamStyle.FORMAT and valid

    @staticmethod
    def __is_valid_statement(sql_statement: str) -> Tuple[Union[bool, Any], Union[str, None]]:
        placeholders = list(KineticaConnection.__extract_parameter_placeholders(sql_statement).keys())

        if len(placeholders) > 1:
            raise ProgrammingError("SQL statement {} contains different parameter placeholder formats {}"
                                   .format(sql_statement, placeholders))

        placeholder = placeholders[0] if len(placeholders) == 1 else None
        supported_paramstyles = [e.value for e in ParamStyle]
        return placeholder is None or placeholder.value in supported_paramstyles, placeholder

    @staticmethod
    def __extract_parameter_placeholders(sql_statement: str):
        # Define regular expression patterns to match different DBAPI v2 placeholders
        patterns = {
            ParamStyle.QMARK: r'\?',  # Question mark style
            ParamStyle.NUMERIC: r':(\d+)',  # Numeric style (e.g., :1, :2)
            ParamStyle.NUMERIC_DOLLAR: r'\$\d+',  # Numeric dollar style (e.g., $1, $2)
            # 'named': r':\w+',  # Named style (e.g., :name)
            ParamStyle.FORMAT: r'%[sdifl]',  # ANSI C printf format codes (e.g., %s)
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
        """ Replace all occurrences of '?' with $1, $2, ..., $n in the SQL statement

        Args:
            query (str): the SQL statement

        Returns:
            str: the modified SQL statement
        """
        
        # Initialize a counter to keep track of the occurrences of '?'
        counter = 1

        # Find the last occurrence of '?'
        last_question_mark_index = query.rfind('?')

        # Replace all occurrences of '?' with $1, $2, ..., $n
        replaced_string = ''
        for char in query:
            if char == '?':
                replaced_string += f'${counter}'
                # Increment the counter until the last occurrence of '?'
                if counter < last_question_mark_index + 1:
                    counter += 1
            else:
                replaced_string += char

        return replaced_string

    @staticmethod
    def __process_params_numeric(query: str):
        """ Replace each ':n' with corresponding '$n' in the SQL statement

        Args:
            query (str): the SQL statement

        Returns:
            str: the modified SQL statement
        """
        
        pattern = r':(\d+)'

        # Find all matches and store unique numbers in a set to avoid duplicates
        matches = sorted(set(re.findall(pattern, query)), key=int)

        # Replace each ':n' with corresponding '$n'
        for i, match in enumerate(matches, start=1):
            query = re.sub(r':{}'.format(match), r'${}'.format(i), query)

        return query

    @staticmethod
    def __process_params_format(query: str):
        """ Replace each ANSI C format specifier with corresponding $n in the SQL statement

        Args:
            query (str): the SQL statement

        Returns:
            str: the modified SQL statement
        """
        
        # Regular expression pattern to match ANSI C format specifiers
        pattern = r'%[sdifl]'

        # Find all matches
        matches = re.findall(pattern, query)

        # Replace each format specifier with corresponding $n
        for i, match in enumerate(matches, start=1):
            query = query.replace(match, f'${i}', 1)

        return query
