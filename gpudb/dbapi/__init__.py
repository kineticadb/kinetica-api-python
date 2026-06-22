"""
Pure Python DB API wrapper around Kinetica Python API.

"""

import logging
from typing import Optional, Union, Dict, Any, overload


from gpudb.dbapi.core import *
from gpudb.dbapi.aio import *
from gpudb.dbapi.pep249.type_constructors import *

# Module-level logger for dbapi
_logger = logging.getLogger("gpudb.dbapi")

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
    **kwargs: Any,
) -> KineticaConnection:
    """The global method to return a Kinetica connection

    Example
    ::

        #  Basic authentication
        con = gpudb.connect(
            "kinetica://",
            url = URL,
            username = USER,
            password = PASS,
            default_schema = SCHEMA,
            options = {"skip_ssl_cert_verification": True}
        )

        #  oauth2 authentication
        con = gpudb.connect(
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

    # Set client_name and client_version for DB API connections
    # If user/framework provides these values, chain them with DBAPI identification
    # Final User-Agent format: "<user_app>/<user_ver> kinetica-dbapi/<api_ver> kinetica-api-python/..."
    if gpudb_options is None:
        gpudb_options = {}

    from gpudb import GPUdb

    dbapi_client_name = "kinetica-dbapi"
    dbapi_client_version = GPUdb.api_version

    user_client_name = gpudb_options.get("client_name")
    user_client_version = gpudb_options.get("client_version")

    if user_client_name and user_client_version:
        # Both provided: build chained User-Agent prefix
        # Format: "user_app/user_ver kinetica-dbapi/api_ver" stored as name, with placeholder version
        # The GPUdb.__build_user_agent_string will produce: "{name}/{version} kinetica-api-python/..."
        # So we put the full chain (minus trailing part) in name and use a version marker
        chained_prefix = f"{user_client_name}/{user_client_version} {dbapi_client_name}"
        gpudb_options["client_name"] = chained_prefix
        gpudb_options["client_version"] = dbapi_client_version
    elif user_client_name or user_client_version:
        # Only one provided: warn and use DBAPI defaults
        _logger.warning(
            "[gpudb.dbapi] Both client_name and client_version must be provided together. "
            "Ignoring partial values (client_name='%s', client_version='%s') and using DBAPI defaults.",
            user_client_name, user_client_version
        )
        gpudb_options["client_name"] = dbapi_client_name
        gpudb_options["client_version"] = dbapi_client_version
    else:
        # Neither provided: use DBAPI defaults
        gpudb_options["client_name"] = dbapi_client_name
        gpudb_options["client_version"] = dbapi_client_version

    return KineticaConnection(
        url=url,
        username=username,
        password=password,
        oauth_token=oauth_token,
        default_schema=default_schema,
        gpudb_options=gpudb_options,
    )
