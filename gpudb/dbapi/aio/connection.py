"""
Async connection object for Kinetica which fits the DB API spec.

"""
from typing import Optional, Union, Sequence
from gpudb.dbapi.core.cursor import Cursor, ParamStyle
from gpudb.dbapi.pep249.exceptions import ProgrammingError
from gpudb.gpudb import GPUdb  # pylint: disable=no-name-in-module
from ..pep249 import aiopep249
from ..pep249.aiopep249 import (
    SQLQuery,
    ProcName,
    ProcArgs,
    QueryParameters,
)
from .cursor import AsyncCursor
from .utils import to_thread


class AsyncKineticaConnection(aiopep249.AsyncCursorExecuteMixin, aiopep249.AsyncConnection):
    """
    A DB API 2.0 compliant async connection for Kinetica, as outlined in
    PEP 249.

    """

    def __init__(
        self,
        param_style: Optional[str] = ParamStyle.NUMERIC_DOLLAR.value,
        *,
        url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        oauth_token: Optional[str] = None,
        connection_options: Optional[dict] = None,
    ):
        """Constructor
        Called by :py:meth:`connect()` in :py:mod:`gpudb.dbapi`

        Args:
            param_style (Optional[str], optional): String constant stating the type of parameter marker formatting
                expected by the interface. Defaults to ParamStyle.NUMERIC_DOLLAR.
            url (Optional[str], optional): the Kinetica URL; has to be keyword only. Defaults to None.
            username (Optional[str], optional): the Kinetica username; has to be keyword only. Defaults to None.
            password (Optional[str], optional): the Kinetica password; has to be keyword only. Defaults to None.
            oauth_token (Optional[str], optional): the oauth2 token; has to be keyword only. Defaults to None.
            connection_options (Optional[dict], optional): Defaults to None.

        Raises:
            ProgrammingError: Raised in case of incorrect parameters passed in
        """
        self._closed = False
        self._param_style = param_style

        if not url or not len(url) > 0:
            raise ProgrammingError("Server url must be given ...")
        options = GPUdb.Options()

        if username and len(username) > 0:
            options.username = username
            options.password = password

        if oauth_token and len(oauth_token) > 0:
            options.oauth_token = oauth_token

        if connection_options and "bypass_ssl_cert_check" in connection_options:
            options.skip_ssl_cert_verification = connection_options[
                "bypass_ssl_cert_check"
            ]

        self._connection = GPUdb(host=url, options=options)


    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def close(self) -> None:
        """Close the database connection."""
        if self._closed:
            return

        self._closed = True

    async def cursor(self) -> AsyncCursor:
        return AsyncCursor(self, Cursor(self))

    async def callproc(
        self, procname: ProcName, parameters: Optional[ProcArgs] = None
    ) -> Optional[ProcArgs]:
        """ This method is used to call a Kinetica procedure.
            The sequence of parameters must contain one entry for each argument 
            that the procedure expects.
            The result of the call is returned as modified copy of the input sequence.

            The procedure may also provide a result set as output.

        Args:
            procname (ProcName): the name of the procedure
            parameters (Optional[ProcArgs], optional): the parameters to the procedure, which Kinetica doesn't support
            as of now. Defaults to None. This is ignored for now

        Returns:
            Optional[ProcArgs]: None
        """
        cursor = await self.cursor()
        return await cursor.callproc(procname, parameters)

    async def execute(
        self, operation: SQLQuery, parameters: Optional[QueryParameters] = None
    ) -> AsyncCursor:
        """The method to execute a single SQL statement (query or command) and return a cursor.

        Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
        Variables are specified in a database-specific notation (see the module's paramstyle attribute for details).

        .. seealso:: :obj:`ParamStyle`

        Args:
            operation (SQLQuery): the SQL statement (query or command) to execute
            parameters (Optional[QueryParameters], optional): the parameters to the query;
                typically a heterogeneous list. Defaults to None.

        Raises:
            ProgrammingError: Raised in case the SQL statement passed in is invalid

        Returns:
            Cursor: a Cursor containing the results of the query
        """
        cursor = await self.cursor()
        return await cursor.execute(operation, parameters)

    async def executemany(
        self, operation: SQLQuery, seq_of_parameters: Sequence[QueryParameters]
    ) -> AsyncCursor:
        """Method used to execute the same statement with a sequence of parameter values.
            The cursor is only returned from the last execution

        .. seealso:: :func:`execute`


        Args:
            operation (SQLQuery): the SQL query
            seq_of_parameters (Sequence[QueryParameters]): a list of parameters (tuples)

        Returns:
            Cursor: a Cursor instance to iterate over the results
        """
        cursor = await self.cursor()
        return await cursor.executemany(operation, seq_of_parameters)

    async def executescript(self, script: SQLQuery) -> AsyncCursor:
        """This method executes an SQL script which is a ';' separated list of SQL statements.

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
                cursor = await self.execute(sql_statement)
                cursor_list.append(cursor)
            last_cursor = cursor_list[:-1][0]

            for cursor in cursor_list:
                await cursor.close()

        return last_cursor

    @property
    def connection(self):
        return self._connection

    @property
    def closed(self):
        return self._closed
