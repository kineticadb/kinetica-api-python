"""
Pure Python DB API wrapper around Kinetica Python API.

"""

from typing import Optional, Union, Dict, Any, overload


from gpudb.dbapi.core import *
from gpudb.dbapi.aio import *
from gpudb.dbapi.pep249.type_constructors import *

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "aconnect",
    "KineticaConnection",
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


def connect(
    connection_string: str = "kinetica://",
    **kwargs: Dict[str, Any],
) -> KineticaConnection:
    """The global method to return a Kinetica connection

    Example
    ::

        #  Basic authentication
        con = gpudb.connect("kinetica://",
                        url=URL,
                        username=USER,
                        password=PASS,
                        bypass_ssl_cert_check=BYPASS_SSL_CERT_CHECK,
                        )

        #  oauth2 authentication
        con = gpudb.connect("kinetica://",
                        url=URL,
                        oauth_token="token_Value",
                        bypass_ssl_cert_check=BYPASS_SSL_CERT_CHECK,
                        )

    Args:
        connection_string (str, optional): the connection string which must be 'kinetica://'
        **kwargs (Dict[str, Any]): the arguments passed to the overloaded method :meth:`connect`

    Raises:
        ProgrammingError: Raised in case wrong connection parameters are detected or a connection fails for some
        other reason

    Returns:
        KineticaConnection: a :class:`KineticaConnection` instance
    """
    if connection_string is None or connection_string != "kinetica://":
        raise ProgrammingError("'connection_string' has to be 'kinetica://'")

    def extract_connect_args(connect_args, *values):
        return (connect_args.get(arg, None) for arg in values)

    url, username, password, oauth_token, options = extract_connect_args(
        kwargs, "url", "username", "password", "oauth_token", "options"
    )

    if not url or not len(url) > 0:
        raise ProgrammingError(
            "Valid 'URL' not found in the 'connect_args' dict ..."
        )
    username = username if username else ""
    password = password if password else ""
    oauth_token = oauth_token if oauth_token else ""

    return KineticaConnection(
        url=url,
        username=username,
        password=password,
        oauth_token=oauth_token,
        connection_options=options if options else {},
    )
