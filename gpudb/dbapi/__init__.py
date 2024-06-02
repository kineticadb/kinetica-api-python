"""
Pure Python DB API wrapper around Kinetica Python API.

"""
from typing import Optional, Union, Dict, Any, overload


from gpudb.dbapi.core import *
from gpudb.dbapi.pep249 import *
from gpudb.dbapi.pep249.type_constructors import *

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Cursor",
    "TimestampFromTicks",
    "TimeFromTicks",
    "DateFromTicks",
    "Date",
    "Time",
    "Timestamp",
    "ROWID",
    "DATETIME",
    "NUMBER",
    "BINARY",
    "STRING",
    "Binary",
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

__version__ = "0.0.1"

# pylint: disable=invalid-name
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


@overload
def connect(connection_string: str = 'kinetica://', *, connect_args: Dict[str, Any] = ...) -> KineticaConnection:
    """The global method to return a Kinetica connection

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

            The keys that are valid for the `options` dict within `connect_args` is the same set that is allowed
            by the class :class:`GPUDB.Options` in the module :py:mod:`gpudb`

    Returns:
        KineticaConnection: an instance of the :class:`KineticaConnection`
    """
    ...


def connect(
        connection_string: str = 'kinetica://',
        **kwargs: Dict[str, Any],
) -> KineticaConnection:
    """The global method to return a Kinetica connection

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
        connection_string (str, optional): the connection string which must be 'kinetica://'
        **kwargs (Dict[str, Any]): the arguments passed to the overloaded method :meth:`connect`

    Raises:
        ProgrammingError: Raised in case wrong connection parameters are detected or a connection fails for some
        other reason

    Returns:
        KineticaConnection: a :class:`KineticaConnection` instance
    """
    if connection_string is None or connection_string != 'kinetica://':
        raise ProgrammingError("'connection_string' has to be 'kinetica://'")

    connection_args = kwargs.pop('connect_args', None)
    if not connection_args or len(connection_args) == 0:
        raise ProgrammingError("'connection_args' cannot be None or empty")

    extract_connect_args = lambda connect_args, *values: (connect_args.get(arg, None) for arg in values)

    url, username, password, options = extract_connect_args(connection_args, 'url', 'username', 'password', 'options')

    if not url or not len(url) > 0:
        raise ProgrammingError("connection_string and url has to be given to connect to Kinetica ...")
    username = username if username else ''
    password = password if password else ''

    return KineticaConnection(url=url, username=username, password=password, connection_options=options if options else {})
