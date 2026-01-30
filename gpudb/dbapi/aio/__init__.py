"""
Pure Python DB API wrapper around Kinetica Python API. This
async implementation uses the sync implementation and asyncio.to_thread.


"""
from typing import Any, Dict, overload
from gpudb.dbapi.pep249.type_constructors import *
from gpudb.dbapi.core.exceptions import (
    DatabaseError,
    DataError,
    Error,
    InterfaceError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from .connection import AsyncKineticaConnection

from .cursor import AsyncCursor

# from .. import __version__

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "aconnect",
    "AsyncKineticaConnection",
    "AsyncCursor",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "IntegrityError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
]

# pylint: disable=invalid-name
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


def aconnect(
    connection_string: str = "kinetica://",
    **kwargs: Any,
) -> AsyncKineticaConnection:
    """The global method to return an async Kinetica connection

    Example
    ::

        #  Basic authentication
        con = gpudb.aconnect(
            "kinetica://",
            url = URL,
            username = USER,
            password = PASS,
            default_schema = SCHEMA,
            options = {"skip_ssl_cert_verification": True}
        )

        #  oauth2 authentication
        con = gpudb.aconnect(
            "kinetica://",
            url = URL,
            oauth_token = "token_value",
            options = {"skip_ssl_cert_verification": True}
        )

    Args:
        connection_string (str, optional): the connection string which must be "kinetica://"
        **kwargs (Dict[str, Any]): the arguments passed to the overloaded method :meth:`connect`

    Raises:
        ProgrammingError: Raised in case wrong connection parameters are
        detected or a connection fails for some other reason

    Returns:
        KineticaConnection: a :class:`KineticaConnection` instance
    """
    def extract_connect_args(connect_args, *values):
        return (connect_args.get(arg, None) for arg in values)

    if connection_string is None or connection_string != "kinetica://":
        raise ProgrammingError("'connection_string' has to be 'kinetica://'")

    url, username, password, oauth_token, default_schema, gpudb_options = extract_connect_args(
        kwargs, "url", "username", "password", "oauth_token", "default_schema", "options"
    )

    if not url or not len(url) > 0:
        raise ProgrammingError("Valid URL not specified ...")

    default_schema = default_schema if default_schema else ""

    return AsyncKineticaConnection(
        url=url,
        username = username,
        password = password,
        oauth_token = oauth_token,
        default_schema = default_schema,
        gpudb_options = gpudb_options,
    )
