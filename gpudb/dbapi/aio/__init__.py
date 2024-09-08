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


@overload
def aconnect(
    connection_string: str = "kinetica://", *, connect_args: Dict[str, Any] = ...
) -> AsyncKineticaConnection:
    """The global method to return an async Kinetica connection

    Example
    ::

        con = gpudb.connect("kinetica://",
                     connect_args={
                         'url': 'http://localhost:9191',
                         'username': 'user',
                         'password': 'password',
                         'options': {'bypass_ssl_cert_check': True},
                     })


    Args:
        connection_string (str): the connection string which must be 'kinetica://'
        connect_args (Dict[str, Any], optional): a mandatory `dict` like

            connect_args={
                'url': 'http://localhost:9191',

                'username': 'user',

                'password': 'password',

                'options': {'bypass_ssl_cert_check': True},
            })

            The keys that are valid for the `options` dict within `connect_args` 
            is the same set that is allowed by the class :class:`GPUDB.Options` 
            in the module :py:mod:`gpudb`

    Returns:
        KineticaConnection: an instance of the :class:`AsyncKineticaConnection`
    """
    ...


def aconnect(
    connection_string: str = "kinetica://",
    **kwargs: Dict[str, Any],
) -> AsyncKineticaConnection:
    """The global method to return an async Kinetica connection

    Example
    ::

        #  Basic authentication
        con = gpudb.connect("kinetica://",
                     connect_args={
                         'url': 'http://localhost:9191',
                         'username': 'user',
                         'password': 'password',
                         'options': {'bypass_ssl_cert_check': True},
                     })

        #  oauth2 authentication
        con = gpudb.connect("kinetica://",
                     connect_args={
                         'url': 'http://localhost:9191',
                         'oauth_token': 'token_value',
                         'options': {'bypass_ssl_cert_check': True},
                     })

    Args:
        connection_string (str, optional): the connection string which must be 'kinetica://'
        **kwargs (Dict[str, Any]): the arguments passed to the overloaded method :meth:`connect`

    Raises:
        ProgrammingError: Raised in case wrong connection parameters are 
        detected or a connection fails for some other reason

    Returns:
        KineticaConnection: a :class:`KineticaConnection` instance
    """
    if connection_string is None or connection_string != "kinetica://":
        raise ProgrammingError("'connection_string' has to be 'kinetica://'")

    connection_args = kwargs.pop("connect_args", None)
    if not connection_args or len(connection_args) == 0:
        raise ProgrammingError("'connection_args' cannot be None or empty")

    def extract_connect_args(connect_args, *values):
        return (connect_args.get(arg, None) for arg in values)

    url, username, password, oauth_token, options = extract_connect_args(
        connection_args, "url", "username", "password", "oauth_token", "options"
    )

    if not url or not len(url) > 0:
        raise ProgrammingError(
            "Valid 'URL' not found in the 'connect_args' dict ..."
        )
    username = username if username else ""
    password = password if password else ""
    oauth_token = oauth_token if oauth_token else ""

    return AsyncKineticaConnection(
        url=url,
        username=username,
        password=password,
        oauth_token=oauth_token,
        connection_options=options if options else {},
    )
