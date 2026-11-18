# ---------------------------------------------------------------------------
# gpudb_http.py - HTTP transport settings for the GPUdb client, as the Java
# API's connection manager applies them.
#
# Copyright (c) 2026 Kinetica DB Inc.
# ---------------------------------------------------------------------------
"""HTTP transport for the GPUdb client: TCP keep-alive on every connection,
and pooled connections that are not reused once they have been idle too long.

``requests`` keeps a pool of connections per host (through ``urllib3``), which
already checks that a pooled connection has not been closed by the server
before reusing it.  What it does not do, and the Java API's connection manager
does, is:

* turn TCP keep-alive on, so that a long-running request behind a NAT, a
  firewall or a load balancer is not silently dropped as idle; and

* stop reusing a connection that has sat idle for a while, which such a device
  may have dropped without the client being told.
"""

import socket
import time

from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool


# A pooled connection idle for longer than this is not reused, as the Java API
# evicts idle connections after 100 seconds
IDLE_CONNECTION_EVICTION_SECONDS = 100


def keep_alive_socket_options( keep_alive, idle = 0, interval = 0, count = 0 ):
    """The socket options of a new connection: urllib3's own, and TCP
    keep-alive if `keep_alive`, with the idle time before the first probe, the
    time between probes and the number of probes before giving up -- each only
    where given (not 0), and where the platform supports it; otherwise the
    operating system's default applies, as in Java.
    """
    options = list( HTTPConnection.default_socket_options )
    if not keep_alive:
        return options

    options.append( (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) )

    # macOS names the idle time TCP_KEEPALIVE
    idle_option = getattr( socket, "TCP_KEEPIDLE", getattr( socket, "TCP_KEEPALIVE", None ) )
    for option, value in ( (idle_option, idle),
                           (getattr( socket, "TCP_KEEPINTVL", None ), interval),
                           (getattr( socket, "TCP_KEEPCNT", None ), count) ):
        if value and (option is not None):
            options.append( (socket.IPPROTO_TCP, option, value) )

    return options
# end keep_alive_socket_options


class _IdleEviction:
    """Marks a connection's return to the pool, and closes one taken from it
    after more than `max_idle` seconds idle; urllib3 then opens it afresh.
    """

    max_idle = IDLE_CONNECTION_EVICTION_SECONDS

    def _put_conn( self, conn ):
        if conn is not None:
            conn._gpudb_idle_since = time.monotonic()
        super()._put_conn( conn )

    def _get_conn( self, timeout = None ):
        conn = super()._get_conn( timeout )
        idle_since = getattr( conn, "_gpudb_idle_since", None )
        if (idle_since is not None) and ((time.monotonic() - idle_since) > self.max_idle):
            conn.close()
        return conn
# end class _IdleEviction


class _HTTPConnectionPool( _IdleEviction, HTTPConnectionPool ):
    pass


class _HTTPSConnectionPool( _IdleEviction, HTTPSConnectionPool ):
    pass


class TransportAdapter( HTTPAdapter ):
    """An HTTP adapter whose connections carry the given socket options (TCP
    keep-alive), and whose pools do not reuse a connection idle for longer
    than `max_idle` seconds.
    """

    def __init__( self, socket_options, max_idle = IDLE_CONNECTION_EVICTION_SECONDS, **kwargs ):
        # Set before the base constructor, which builds the pool manager
        self._gpudb_socket_options = socket_options
        self._gpudb_pool_classes = {
            "http":  type( "HTTPConnectionPool", (_HTTPConnectionPool,), { "max_idle": max_idle } ),
            "https": type( "HTTPSConnectionPool", (_HTTPSConnectionPool,), { "max_idle": max_idle } ),
        }
        super().__init__( **kwargs )
    # end __init__

    def init_poolmanager( self, *args, **kwargs ):
        kwargs[ "socket_options" ] = self._gpudb_socket_options
        super().init_poolmanager( *args, **kwargs )
        self.poolmanager.pool_classes_by_scheme = self._gpudb_pool_classes
    # end init_poolmanager

    def proxy_manager_for( self, proxy, **proxy_kwargs ):
        proxy_kwargs[ "socket_options" ] = self._gpudb_socket_options
        manager = super().proxy_manager_for( proxy, **proxy_kwargs )
        manager.pool_classes_by_scheme = self._gpudb_pool_classes
        return manager
    # end proxy_manager_for
# end class TransportAdapter


def request_timeout( connect_timeout, read_timeout ):
    """The (connect, read) timeout of a request, as requests takes it.

    The connect timeout -- the server connection timeout, in Java too -- is
    never longer than the read timeout, where one is set, so that a request is
    not allowed longer to connect than the caller allowed it in all; 0 means
    no limit on connecting, as in Java.  The read timeout is the caller's, None
    for no limit.
    """
    connect = connect_timeout if connect_timeout else None
    if (read_timeout is not None) and ((connect is None) or (read_timeout < connect)):
        connect = read_timeout
    return (connect, read_timeout)
# end request_timeout
