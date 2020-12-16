###############################################################################
#
# gpudb_multihead_io.py
#
# Python API file for inserting multiple records into GPUdb via one or more
# nodes/heads.
#
# Copyright (c) 2016 GIS Federal
#
###############################################################################

from __future__ import print_function


import inspect
import sys
import traceback

# We'll need to do python 2 vs. 3 things in many places
IS_PYTHON_3 = (sys.version_info[0] >= 3) # checking the major component
IS_PYTHON_27_OR_ABOVE = sys.version_info >= (2, 7)


if IS_PYTHON_3:
    from gpudb.gpudb import GPUdb, GPUdbRecord, GPUdbRecordType, GPUdbColumnProperty, RecordType, _Util
    from gpudb.gpudb import GPUdbException, GPUdbConnectionException, GPUdbExitException, GPUdbFailoverDisabledException, GPUdbHAUnavailableException, GPUdbUnauthorizedAccessException
else:
    from gpudb       import GPUdb, GPUdbRecord, GPUdbRecordType, GPUdbColumnProperty, RecordType, _Util
    from gpudb       import GPUdbException, GPUdbConnectionException, GPUdbExitException, GPUdbFailoverDisabledException, GPUdbHAUnavailableException, GPUdbUnauthorizedAccessException

from avro import schema, datafile, io
import builtins
import datetime
import json
import logging
import random
import re
import struct
import time
import uuid

from protocol import Record


try:
    # if this fails, use the slower pure python implementation
    import mmh3 # murmur hash
    from mmh3 import hash_bytes, hash64 # murmur hash
except:

    import os
    # The absolute path of this gpudb.py module for importing local packages
    gpudb_module_path = __file__
    if gpudb_module_path[len(gpudb_module_path)-3:] == "pyc": # allow symlinks to gpudb.py
        gpudb_module_path = gpudb_module_path[0:len(gpudb_module_path)-1]
    if os.path.islink(gpudb_module_path): # allow symlinks to gpudb.py
        gpudb_module_path = os.readlink(gpudb_module_path)
    gpudb_module_path = os.path.dirname(os.path.abspath(gpudb_module_path))

    # Search for our modules first, probably don't need imp or virt envs.
    if not gpudb_module_path + "/packages" in sys.path :
        sys.path.insert(1, gpudb_module_path + "/packages")

    # pure python implementation
    import pymmh3 as mmh3
# end try block


# Python version dependent imports
if IS_PYTHON_27_OR_ABOVE:
    import collections
else:
    import ordereddict as collections # a separate package

if IS_PYTHON_3:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


# Handle basestring in python3
if IS_PYTHON_3:
    long = int
    basestring = str
    class unicode:
        pass

# -----------------------------------------------------------------
#                            Logging
# -----------------------------------------------------------------
# -----------------------------
# Add a trace method
# -----------------------------
logging.TRACE = 9
logging.addLevelName( logging.TRACE, "TRACE" )

def trace( self, message, *args, **kws ):
    if self.isEnabledFor( logging.TRACE ):
        # Yes, logger takes its '*args' as 'args'
        self._log( logging.TRACE, message, args, **kws )
    # end if
# end def trace

logging.Logger.trace = trace

# -----------------------------------------------
# Logging utility for helper classes in this file
# -----------------------------------------------
mh_io_log  = logging.getLogger( "gpudb.MultiHeadIO" )
handler    = logging.StreamHandler()
formatter  = logging.Formatter( "%(asctime)s %(levelname)-8s %(message)s",
                                 "%Y-%m-%d %H:%M:%S" )
handler.setFormatter( formatter )
mh_io_log.addHandler( handler )

# Prevent logging statements from being duplicated
mh_io_log.propagate = True
# mh_io_log.propagate = False


def mh_log_debug( message ):
    # Get calling method's information from the stack
    stack = inspect.stack()
    # stack[1] gives the previous/calling function
    filename = stack[1][1].split("/")[-1]
    ln       = stack[1][2]
    func     = stack[1][3]

    mh_io_log.debug( "[gpudb_multihead_io::{fn}::{line}::{func}]  {msg}"
                     "".format( fn = filename,
                                func = func, line = ln,
                                msg = message ) )
# end mh_log_debug

def mh_log_warn( message ):
    mh_io_log.warn( "[gpudb_multihead_io] {}".format( message ) )
# end mh_log_warn

def mh_log_info( message ):
    mh_io_log.info( "[gpudb_multihead_io] {}".format( message ) )
# end mh_log_info

def mh_log_error( message ):
    mh_io_log.error( "[gpudb_multihead_io] {}".format( message ) )
# end mh_log_error
# ------------------------------------------------------------------------



# Some string constants used throughout the program
class C:
    """Some string constants used throughout the program."""

    # JSON dict keys
    _name   = "name"
    _type   = "type"
    _fields = "fields"
    _is_nullable = "is_nullable"

    # gpudb response status related dict keys
    _status_info = "status_info"
    _msg         = "message"
    _status      = "status"
    _error       = "ERROR"
    _ok          = "OK"

    # GPUdb /admin/show/shards response dict keys
    _shard_ranks = "rank"
    _shard_version = "version"

    # GPUdb /insert/records response constants
    _count_inserted = "count_inserted"
    _count_updated  = "count_updated"

    # GPUdb /insert/records and /get/records response common constants
    _info           = "info"
    _data_rerouted  = "data_rerouted"
    _true           = "true"

    # GPUdb /show/table response dict keys
    _table_names = "table_names"
    _table_properties   = "properties"
    _table_descriptions = "table_descriptions"
    _type_schemas  = "type_schemas"
    _pk        = "primary_key"
    _shard_key = "shard_key"
    _COLLECTION = "COLLECTION"

    # GPUdb /system/properties response dict keys
    _sys_properties    = "property_map"
    _multihead_enabled = "conf.enable_worker_http_servers"
    _worker_URLs       = "conf.worker_http_server_urls"
    _worker_IPs        = "conf.worker_http_server_ips"
    _worker_ports      = "conf.worker_http_server_ports"

    # Various string values
    _FALSE = "FALSE"
    _TRUE  = "TRUE"
    _False = "False"
    _True  = "True"
    _false = "false"
    _true  = "true"

# end class C


# Exception: InsertionException
# =============================
class InsertionException(Exception):
    """Handles errors during insertion of records into GPUdb.
    """
    def __init__( self, message, records ):
        # call the base class constructor
        super(InsertionException, self).__init__( message )

        self.records = records
    # end __init__

    def get_records( self ):
        return self.records
    # end get_records
# end class InsertionException


# Public Class GPUdbWorkerList
# ============================

class GPUdbWorkerList:
    """A list of worker URLs to use for multi-head ingest."""

    def __init__( self, gpudb, ip_regex = None,
                  use_head_node_only = False ):
        """Automatically populates the GPUdbWorkerList object with the worker
        URLs for the GPUdb server to support multi-head ingest. (If the
        specified GPUdb instance has multi-head ingest disabled, the worker
        list will have the head node URL only and multi-head ingest will
        not be used.)

        Note that in some cases, workers may be configured to use more than one
        IP address, not all of which may be accessible to the client; this
        constructor uses the first IP returned by the server for each
        worker.

        Parameters:
        gpudb (GPUdb)
            The GPUdb client handle from which to obtain the worker URLs.
        ip_regex (str)
            Optional IP regular expression to match for the worker URLs.
        use_head_node_only (bool)
            Optional boolean flag indicating that only head node should be
            used (for whatever reason), instead of the workers utilizing the
            multi-head feature.
        """
        # Validate the input parameter 'gpudb'
        assert isinstance(gpudb, GPUdb), ("Parameter 'gpudb' must be of "
                                          "type GPUdb; given %s"
                                          % type(gpudb) )
        # Validate the input parameter 'use_head_node_only'
        assert isinstance(use_head_node_only, bool), \
            ("Parameter 'use_head_node_only' must be a boolean value;  given "
             "%s" % str( type( use_head_node_only ) ) )

        self.worker_urls = []
        self.use_head_node_only = use_head_node_only
        self._ip_regex = ip_regex

        # Get system properties
        system_prop_rsp = gpudb.show_system_properties()
        if system_prop_rsp[ C._status_info ][ C._status ] == C._error:
            raise GPUdbException( "Unable to retrieve system properties; error:"
                                  " %s" % system_prop_rsp[ C._status_info ][ C._msg ] )

        system_properties = system_prop_rsp[ C._sys_properties ]

        # Is multi-head ingest enabled on the server?
        if C._multihead_enabled not in system_properties:
            raise GPUdbException( "Missing value for %s" % C._multihead_enabled)

        self._is_multihead_enabled = (system_properties[ C._multihead_enabled ] == C._TRUE)
        if not self._is_multihead_enabled:
            # Multihead ingest is not enabled.  Just return the main/only ingestor
            self.worker_urls.append( gpudb.get_url() )
            return # nothing to do

        # Head node-only usage is requested; so just return the head node
        if self.use_head_node_only:
            self.worker_urls.append( gpudb.get_url() )
            return # nothing to do

        # Get the worker URLs (per rank)
        if C._worker_URLs in system_properties:
            self.worker_URLs_per_rank = system_properties[ C._worker_URLs ].split( ";" )

            # Process the URLs per worker rank (ignoring rank-0)
            for i in list( range(1, len(self.worker_URLs_per_rank)) ):
                urls_per_rank = self.worker_URLs_per_rank[ i ]

                # Check if this rank has been removed
                if not urls_per_rank:
                    # We need an empty slot to indicate removed ranks
                    self.worker_urls.append( None )
                    continue

                url_addresses_for_this_rank = urls_per_rank.split( "," )
                found = False

                # Check each URL
                for url_str in url_addresses_for_this_rank:
                    # Parse the URL
                    url = urlparse( url_str )
                    if ((not url.scheme) or (not url.hostname) or (not url.port)):
                        raise GPUdbException("Malformed URL: '{}'".format( url_str ) )

                    if not ip_regex: # no regex given
                        # so, include all IP addresses
                        self.worker_urls.append( url_str )
                        found = True
                        # skip the rest of IP addresses for this rank
                        break
                    else: # check for matching regex
                        match = re.match(ip_regex, url_str)
                        if match: # match found
                            self.worker_urls.append( url_str )
                            found = True
                            # skip the rest of IP addresses for this rank
                            break
                        # end found match
                    # end if-else
                # end inner loop

                # if no worker found for this rank, throw exception
                if not found:
                    raise GPUdbException("No matching URL found for worker"
                                     "%d." % i)
            # end inner loop
        else: # Need to process the separately given IP addresses and ports

            # Get the worker IP addresses (per rank)
            if C._worker_IPs not in system_properties:
                raise GPUdbException( "Missing value for %s" % C._worker_IPs)

            self.worker_IPs_per_rank = system_properties[ C._worker_IPs ].split( ";" )

            # Get the worker ports
            if C._worker_ports not in system_properties:
                raise GPUdbException( "Missing value for %s" % C._worker_ports)

            self.worker_ports = system_properties[ C._worker_ports ].split( ";" )

            # Check that the IP and port list lengths match
            if (len(self.worker_IPs_per_rank) != len(self.worker_ports)):
                raise GPUdbException("Inconsistent number of values for %s and %s."
                                 % (C._worker_IPs_per_rank, C._worker_ports) )

            # Get the protocol used for the client (HTTP or HTTPS?)
            protocol = "https://" if (gpudb.connection == "HTTPS") else "http://"

            # Process the IP addresses per worker rank (ignoring rank-0)
            for i in list( range(1, len(self.worker_IPs_per_rank)) ):
                ips_per_rank = self.worker_IPs_per_rank[ i ]

                # Check if this rank has been removed
                if not ips_per_rank:
                    # We need an empty slot to indicate removed ranks
                    self.worker_urls.append( None )
                    continue

                ip_addresses_for_this_rank = ips_per_rank.split( "," )
                found = False

                # Check each IP address
                for ip_address in ip_addresses_for_this_rank:
                    # Validate the IP address's syntax
                    if not self.validate_ip_address( ip_address ):
                        raise GPUdbException( "Malformed IP address: %s" % ip_address )

                    # Generate the URL using the IP address and the port
                    url = (protocol + ip_address + ":" + self.worker_ports[i])

                    if (ip_regex == ""): # no regex given
                        # so, include all IP addresses
                        self.worker_urls.append( url )
                        found = True
                        # skip the rest of IP addresses for this rank
                        break
                    else: # check for matching regex
                        match = re.match(ip_regex, ip_address)
                        if match: # match found
                            self.worker_urls.append( url )
                            found = True
                            # skip the rest of IP addresses for this rank
                            break
                        # end found match
                    # end if-else
                # end inner loop

                # if no worker found for this rank, throw exception
                if not found:
                    raise GPUdbException("No matching IP address found for worker"
                                         "%d." % i)
            # end inner loop
        # end if-else

        # if no worker found, throw error
        if not self.worker_urls:
            raise GPUdbException( "No worker HTTP servers found." )
    # end GPUdbWorkerList __init__


    def __str__( self ):
        """String representation of the worker list.
        """
        return "{}".format( [ str(url) for url in self.worker_urls ] )
    # end __str__


    def __eq__( self, other ):
        """Override the equality operator.
        """
        # Check the type of the other object
        if not isinstance( other, GPUdbWorkerList ):
            return False

        if ( set(self.worker_urls) != set(other.worker_urls) ):
            return False

        return True
    # end __eq__


    def __ne__( self, other ):
        """Override the inequality operator.
        """
        return not self.__eq__( other )
    # end __ne__


    def validate_ip_address( self, ip_address ):
        """Validates the input string as an IP address (accepts IPv4 only).

        @param ip_address  String that needs to be validated.

        Returns true or false.
        """
        try:
            parts = ip_address.split('.')
            # Accepting IPv4 for now only
            return ( (len(parts) == 4)
                     and all(0 <= int(part) < 256 for part in parts) )
        except ValueError:
            return False
        except (AttributeError, TypeError):
            return False
    # end validate_ip_address


    def get_worker_urls( self ):
        """Returns a list of the URLs for the GPUdb workers."""
        return self.worker_urls
    # end get_worker_urls


    def is_multihead_enabled( self ):
        """Returns whether multi-head I/O is enabled at the server."""
        return self._is_multihead_enabled
    # end is_multihead_enabled


    def get_ip_regex( self ):
        """Returns the IP regex, if any, used to create the worker list."""
        return self._ip_regex
    # end get_ip_regex
# end class GPUdbWorkerList




# Internal Class _ColumnTypeSize
# ==============================
class _ColumnTypeSize:
    """Contains type size information in bytes.
    """
    CHAR1     =   1
    CHAR2     =   2
    CHAR4     =   4
    CHAR8     =   8
    CHAR16    =  16
    CHAR32    =  32
    CHAR64    =  64
    CHAR128   = 128
    CHAR256   = 256
    DATE      =   4
    DATETIME  =   8
    DECIMAL   =   8
    DOUBLE    =   8
    FLOAT     =   4
    INT       =   4
    INT8      =   1
    INT16     =   2
    IPV4      =   4
    LONG      =   8
    STRING    =   8
    TIME      =   4
    TIMESTAMP =   8
    ULONG     =   8
    UUID      =  16

    # A dict mapping column types to its size in bytes
    column_type_sizes = collections.OrderedDict()
    column_type_sizes[ "char1"    ] =   1
    column_type_sizes[ "char2"    ] =   2
    column_type_sizes[ "char4"    ] =   4
    column_type_sizes[ "char8"    ] =   8
    column_type_sizes[ "char16"   ] =  16
    column_type_sizes[ "char32"   ] =  32
    column_type_sizes[ "char64"   ] =  64
    column_type_sizes[ "char128"  ] = 128
    column_type_sizes[ "char256"  ] = 256
    column_type_sizes[ "date"     ] =   4
    column_type_sizes[ "datetime" ] =   8
    column_type_sizes[ "decimal"  ] =   8
    column_type_sizes[ "ipv4"     ] =   4
    column_type_sizes[ "int8"     ] =   1
    column_type_sizes[ "int16"    ] =   2
    column_type_sizes[ "time"     ] =   4
    column_type_sizes[ "timestamp"] =   8
    column_type_sizes[ "int"      ] =   4
    column_type_sizes[ "double"   ] =   8
    column_type_sizes[ "float"    ] =   4
    column_type_sizes[ "long"     ] =   8
    column_type_sizes[ "string"   ] =   8
    column_type_sizes[ "ulong"    ] =   8
    column_type_sizes[ "uuid"     ] =  16
# end class _ColumnTypeSize


# Internal Class _RecordKey
# =========================
class _RecordKey:
    """Represents a record key for ingestion jobs to GPUdb.  It will
    be used to check for uniqueness before sending the insertion job
    to the server.
    """


    def __init__( self, buffer_size ):
        """Initialize the RecordKey.
        """
        if (buffer_size < 1):
            raise GPUdbException( "Buffer size must be greater than "
                                  "or equal to 1; given %d" % buffer_size )

        # self.record_key = {}
        self._current_size = 0
        self._buffer_size  = buffer_size
        self._buffer_value = bytearray()
        self._is_valid = True

        # The hash value for this record key (used internally in the python API)
        self._hash_code = 0

        # The hash value for routing the record to the appropriate GPUdb worker
        self._routing_hash = 0

        # Minimum and maximum supported years for the date format
        self._MIN_SUPPORTED_YEAR = 1000
        self._MAX_SUPPORTED_YEAR = 2900

        # Some regular expressions needed later
        self._ipv4_regex = re.compile( r"^(?P<a>\d{1,3})\.(?P<b>\d{1,3})\.(?P<c>\d{1,3})\.(?P<d>\d{1,3})$" )
        self._decimal_regex = re.compile( r"^\s*(?P<sign>[+-]?)((?P<int>\d+)(\.(?P<frac1>\d{0,4}))?|\.(?P<frac2>\d{1,4}))\s*\Z" )
    # end RecordKey __init__

    @property
    def is_valid( self ):   # read-only
        """Is the key valid?"""
        return self._is_valid
    # end is_valid

    @property
    def hash_code( self ):  # read-only
        """The hash code for the record key."""
        return self._hash_code
    # end hash_code


    def __is_buffer_full( self, throw_if_full = True ):
        """Internal function which checks whether the buffer is already full.
        """
        if ( len( self._buffer_value ) == self._buffer_size ):
        # if (self._current_size == self._buffer_size):
            if throw_if_full:
                raise GPUdbException( "The buffer is already full!" )
            return True  # yes, buffer full, but we haven't thrown

        return False # buffer NOT full
    # end __is_buffer_full


    def __will_buffer_overflow( self, n, throw_if_overflow = True ):
        """Internal function which checks if the buffer will overflow
        if we attempt to add n more bytes.
        """
        if not isinstance(n, int):
            raise GPUdbException( "Argument 'n' must be an integer, given %s"
                             % str( type( n ) ) )
        if (n < 0):
            raise GPUdbException( "Argument 'n' must be greater than or equal"
                              " to zero; given %d" % n )
        if ( (len( self._buffer_value ) + n) > self._buffer_size ):
        # if ( (self._current_size + n) > self._buffer_size ):
            if throw_if_overflow:
                raise GPUdbException( "The buffer (of size {s}) does not "
                                      "have sufficient room in it to put {n} "
                                      "more byte(s) (current size is {curr})."
                                      "".format( s    = self._buffer_size,
                                                 n    = n,
                                                 curr = len( self._buffer_value ) ) )
                                                 # curr = self._current_size ) )
            return True # yes, will overflow, but we haven't thrown

        return False # buffer will NOT overflow
    # end __will_buffer_overflow


    # We need different versions of the following 2 functions for python 2.x vs. 3.x
    # Note: Choosing to have two different definitions even though the difference
    #       is only in one line to avoid excessive python version check per func
    #       call.
    if IS_PYTHON_3:

        def add_charN( self, val, N ):
            """Add a charN string to the buffer (can be null)--N bytes.
            """
            if (val and (len( val ) > N)): # not a null and too long
                raise GPUdbException( "Char{N} given too long a value: {val}"
                                      "".format( N = N, val = val ) )
            # charN is N bytes long
            self.__will_buffer_overflow( N )

            # Handle nulls
            if val is None:
                for i in list( range( 0, N ) ):
                    self._buffer_value += struct.pack( "=b", 0 )
                return
            # end if

            byte_count = len( val )

            # Trim the string if longer than
            if byte_count > N:
                byte_count = N

            # First, pad with any zeroes "at the end"
            for i in list( range(N, byte_count, -1) ):
                self._buffer_value += struct.pack( "=b", 0 )


            # Then, put the string in little-endian order
            b = bytes( val[-1::-1], "utf-8" )
            self._buffer_value += b
        # end add_charN

    else: # python 2.x


        def add_charN( self, val, N ):
            """Add a charN string to the buffer (can be null)--N bytes.
            """
            if (val and (len( val ) > N)): # not a null and too long
                raise GPUdbException( "Char{N} given too long a value: {val}"
                                      "".format( N = N, val = val ) )

            # charN is N bytes long
            self.__will_buffer_overflow( N )

            # Handle nulls
            if val is None:
                for i in list( range( 0, N ) ):
                    self._buffer_value += struct.pack( "=b", 0 )
                return
            # end if

            # Convert the string to little endian
            # -----------------------------------
            if isinstance( val, unicode ):
                val = str( val )
            byte_count = len( val )

            # Trim the string if longer than
            if byte_count > N:
                byte_count = N

            # First, pad with any zeroes "at the end"
            for i in list( range(N, byte_count, -1) ):
                self._buffer_value += struct.pack( "=b", 0 )


            # Then, put the string in little-endian order
            self._buffer_value += val[-1::-1]
        # end add_charN

    # end if-else for python version



    def add_char1( self, val ):
        """Add a char1 string to the buffer (can be null)--one byte.
        """
        self.add_charN( val, 1 )
    # end add_char1


    def add_char2( self, val ):
        """Add a char2 string to the buffer (can be null)--two bytes.
        """
        self.add_charN( val, 2 )
    # end add_char2


    def add_char4( self, val ):
        """Add a char4 string to the buffer (can be null)--four bytes.
        """
        self.add_charN( val, 4 )
    # end add_char4


    def add_char8( self, val ):
        """Add a char8 string to the buffer (can be null)--eight bytes.
        """
        self.add_charN( val, 8 )
    # end add_char8


    def add_char16( self, val ):
        """Add a char16 string to the buffer (can be null)--16 bytes.
        """
        self.add_charN( val, 16 )
    # end add_char16


    def add_char32( self, val ):
        """Add a char32 string to the buffer (can be null)--32 bytes.
        """
        self.add_charN( val, 32 )
    # end add_char32


    def add_char64( self, val ):
        """Add a char64 string to the buffer (can be null)--64 bytes.
        """
        self.add_charN( val, 64 )
    # end add_char64


    def add_char128( self, val ):
        """Add a char128 string to the buffer (can be null)--128 bytes.
        """
        self.add_charN( val, 128 )
    # end add_char128


    def add_char256( self, val ):
        """Add a char256 string to the buffer (can be null)--256 bytes.
        """
        self.add_charN( val, 256 )
    # end add_char256


    def add_double( self, val ):
        """Add a double to the buffer (can be null)--eight bytes.
        """
        # Doubles are eight bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.DOUBLE )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=d", 0 )
            return
        # end if

        # Add the eight bytes of the double
        self._buffer_value += struct.pack( "=d", float(val) )
    # end add_double


    def add_float( self, val ):
        """Add a float to the buffer (can be null)--four bytes.
        """
        # Floats are four bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.FLOAT )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=f", 0 )
            return
        # end if

        # Add the four bytes of the float
        self._buffer_value += struct.pack( "=f", float(val) )
    # end add_float


    def add_int( self, val ):
        """Add an integer to the buffer (can be null)--four bytes.
        """
        # ints are four bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.INT )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=i", 0 )
            return
        # end if

        # Add each of the four bytes of the integer
        self._buffer_value += struct.pack( "=i", int(val) )
    # end add_int


    def add_int8( self, val ):
        """Add an int8 to the buffer (can be null)--one byte.
        """
        # int8s are one byte long
        self.__will_buffer_overflow( _ColumnTypeSize.INT8 )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=b", 0 )
            return
        # end if

        # Add the byte of the int8
        self._buffer_value += struct.pack( "=b", int(val) )
    # end add_int8


    def add_int16( self, val ):
        """Add an int16 to the buffer (can be null)--two bytes.
        """
        # int16s two one byte long
        self.__will_buffer_overflow( _ColumnTypeSize.INT16 )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=h", 0 )
            return
        # end if

        # Add the byte of the int8
        self._buffer_value += struct.pack( "=h", int(val) )
    # end add_int8



    def add_long( self, val ):
        """Add a long to the buffer (can be null)--eight bytes.
        """
        # Longs are eight bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.LONG )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=q", 0 )
            return
        # end if

        # Add the eight bytes of the long
        self._buffer_value += struct.pack( "=q", long(val) )
    # end add_long


    # We need two different versions for this function based on the python version
    # Note: Choosing to have two different definitions even though the difference
    #       is only in one line to avoid excessive python version check per func
    #       call.
    if IS_PYTHON_3:
        def add_string( self, val ):
            """Add the hash value of the given string to the buffer (can be
            null)--eight bytes.
            """
            # Longs are eight bytes long
            self.__will_buffer_overflow( _ColumnTypeSize.STRING )

            # Handle nulls
            if val is None:
                # Adding a 0 long value
                self._buffer_value += struct.pack( "=q", 0 )
                return
            # end if

            # Hash the string value
            a = mmh3.hash64( bytes(val, "utf-8"), seed = 10 )

            hash_val = a[ 0 ] # the first half

            # Add the eight bytes of the long hash value
            self._buffer_value += struct.pack( "=q", hash_val )
        # end add_string

    else: # Python 2.x
        def add_string( self, val ):
            """Add the hash value of the given string to the buffer (can be
            null)--eight bytes.
            """
            # Longs are eight bytes long
            self.__will_buffer_overflow( _ColumnTypeSize.STRING )

            # Handle nulls
            if val is None:
                # Adding a 0 long value
                self._buffer_value += struct.pack( "=q", 0 )
                return
            # end if

            # Hash the string value
            a = mmh3.hash64( val, seed = 10 )

            hash_val = a[ 0 ] # the first half

            # Add the eight bytes of the long hash value
            self._buffer_value += struct.pack( "=q", hash_val )
        # end add_string
    # end add_string() python version specific


    def add_date( self, val ):
        """Add a date (given as a string or in a date stuct) to the buffer
        (can be null)--four bytes.

        Parameters:
            val (str or datetime.datetime.date)
                The date to add; if string, then in the format of "YYYY-MM-DD".
                The year must be withing the range [1000, 2900].
        """
        # ints are four bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.DATE )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=i", 0 )
            return
        # end if

        # For string values, convert to a date object
        if isinstance( val, basestring ):
            try:
                val = datetime.datetime.strptime( val, '%Y-%m-%d' ).date()
            except ValueError as e:
                # Date not in the correct format; so the key is invalid
                self._buffer_value += struct.pack( "=i", 0 )
                self._is_valid = False
                return
        # end if

        # The server supports years in the range [1000, 2900]
        if (val.year < self._MIN_SUPPORTED_YEAR) or (val.year > self._MAX_SUPPORTED_YEAR):
            self._buffer_value += struct.pack( "=i", 0 )
            self._is_valid = False
            return
        # end if

        # Encode the date struct's value properly
        time_tuple = val.timetuple()
        adjusted_day_of_week = ( ( ( time_tuple.tm_wday + 1 ) % 7 ) + 1 )
        date_integer = ( ( ( val.year - 1900 )  << 21 )
                         | ( val.month          << 17 )
                         | ( val.day            << 12 )
                         | ( (time_tuple.tm_yday) <<  3 )
                         # need to fix day-of-week to match the server's calculation
                         | adjusted_day_of_week )

        # Add each of the four bytes of the integer
        self._buffer_value += struct.pack( "=i", date_integer )
    # end add_date



    def add_datetime( self, val ):
        """Add a datetime (given as a string or in a date stuct) to the buffer
        (can be null)--four bytes.

        Parameters:
            val (str or datetime.datetime.date)
                The date to add; if string, then in the format of
                'YYYY-MM-DD [HH:MM:SS[.mmm]]' where the time and the millisecond
                are optional.
                The allowable range is '1000-01-01 00:00:00.000' through
                '2900-01-01 23:59:59.999'.
                The year must be withing the range [1000, 2900].
        """
        # ints are four bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.DATETIME )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=q", 0 )
            return
        # end if

        # For string values, convert to a date object
        if isinstance( val, basestring ):
            try:
                # Time is optional
                if ":" in val: # So, the time is given
                    # Handle the optional millisecond part
                    if "." in val:
                        # Convert the milliseconds to microseconds
                        val += "000"
                    else: # No milli seconds given
                        val += ".000"
                    # end if-else

                    val = datetime.datetime.strptime( val, '%Y-%m-%d %H:%M:%S.%f' )

                else: # only date, no time given
                    val = val.strip()
                    val = datetime.datetime.strptime( val, '%Y-%m-%d' )
            except ValueError as e:
                # Date not in the correct format; so the key is invalid
                self._buffer_value += struct.pack( "=q", 0 )
                self._is_valid = False
                return
        # end if


        # The server supports years in the range [1000, 2900]
        if (val.year < self._MIN_SUPPORTED_YEAR) or (val.year > self._MAX_SUPPORTED_YEAR):
            self._buffer_value += struct.pack( "=q", 0 )
            self._is_valid = False
            return
        # end if

        # Encode the date struct's value properly
        time_tuple = val.timetuple()
        # Need to fix day-of-week to match the server's calculation
        adjusted_day_of_week = int( ( ( time_tuple.tm_wday + 1 ) % 7 ) + 1 )

        # Encode the datetime just the way the server does it
        datetime_integer = ( ( ( val.year - 1900 )        << 53 )
                             + ( val.month                << 49 )
                             + ( val.day                  << 44 )
                             + ( val.hour                 << 39 )
                             + ( val.minute               << 33 )
                             + ( val.second               << 27 )
                             + ( int(val.microsecond / 1000) << 17 )
                             + ( time_tuple.tm_yday       <<  8 )
                             + ( adjusted_day_of_week     <<  5 ) )

        # Add each of the four bytes of the integer
        self._buffer_value += struct.pack( "=q", datetime_integer )
    # end add_datetime



    def add_decimal( self, val ):
        """Add a decimal number to the buffer (can be null)--eight bytes.

        Parameters:
            val (str)
                Must represent a decimal value up to 19 digits of precision and
                four digits of scale.
        """
        # ints are four bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.DECIMAL )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=Q", 0 )
            return
        # end if

        # Parse the IPv4
        match = self._decimal_regex.match( val )
        if not match:
            # Incorrect format; so we have an invalid key
            self._buffer_value += struct.pack( "=q", 0 )
            self._is_valid = False
            return
        # end if

        # Parse the string to get the decimal value
        decimal_value = 0
        try:
            # Extract the integral and fractional parts, if any
            values = match.groupdict()
            integral_part = int( values[ "int" ] ) if values[ "int" ] else 0
            fraction      = values[ "frac1" ] if values[ "frac1" ] else \
                            ( values[ "frac2" ] if values[ "frac2" ] else "")
            sign = values[ "sign" ]

            # Get the integral part of the decimal value
            decimal_value = integral_part * 10000

            # Put together the integral and fractional part
            frac_len = len( fraction )
            if (frac_len > 0):
                fractional_part = int( fraction ) * (10**(4 - frac_len))
                decimal_value = (integral_part * 10000 ) + fractional_part
            # end if

            # Incorporate the sign
            if (sign == "-"):
                decimal_value = -decimal_value
        except:
            # Incorrect format; so we have an invalid key
            self._buffer_value += struct.pack( "=q", 0 )
            self._is_valid = False
            return
        # end try-catch

        # Add each of the four bytes of the integer
        self._buffer_value += struct.pack( "=q", decimal_value )
    # end add_decimal



    def add_ipv4( self, val ):
        """Add a IPv4 address to the buffer (can be null)--four bytes.

        Parameters:
            val (str)
                Must be in the form of "A.B.C.D" where A, B, C, and D are
                between 0 and 255, inclusive (e.g. 127.0.0.1).
        """
        # ints are four bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.IPV4 )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=I", 0 )
            return
        # end if

        # Parse the IPv4
        match = self._ipv4_regex.match( val )
        if not match:
            # Incorrect format; so we have an invalid key
            self._buffer_value += struct.pack( "=I", 0 )
            self._is_valid = False
            return
        # end if

        # Extract the four integers
        values = match.groupdict()
        a = int( values[ "a" ] )
        b = int( values[ "b" ] )
        c = int( values[ "c" ] )
        d = int( values[ "d" ] )

        # Check that the value does not exceed 255 (no minus
        # sign allowed in the regex, so no worries about negative values)
        if (a > 255) or (b > 255) or (c > 255) or (d > 255):
            self._buffer_value += struct.pack( "=I", 0 )
            self._is_valid = False
            return
        # end if

        # Deduce the integer representing the IPv4 address
        ipv4_integer = ( (   a << 24 )
                         | ( b << 16 )
                         | ( c <<  8 )
                         |   d )

        # Add each of the four bytes of the integer
        self._buffer_value += struct.pack( "=I", ipv4_integer )
    # end add_ipv4




    def add_time( self, val ):
        """Add a time to the buffer (can be null)--four bytes.

        Parameters:
            val (str)
                Must be in the form of "HH:MM:SS[.mmm]" where the
                millisdeconds are optional.
        """
        # ints are four bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.TIME )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=i", 0 )
            return
        # end if

        # For string values, convert to a time object
        if isinstance( val, basestring ):
            try:
                if "." in val:
                    # Convert the milliseconds to microseconds
                    val += "000"
                else: # No milli seconds given
                    val += ".000"

                # Convert the string into a time object
                val = datetime.datetime.strptime( val, '%H:%M:%S.%f' ).time()
            except ValueError as e:
                # Date not in the correct format; so the key is invalid
                self._buffer_value += struct.pack( "=i", 0 )
                self._is_valid = False
                return
        # end if

        # Encode the time struct's value properly
        time_integer = ( ( val.hour   << 26 )
                         | ( val.minute << 20 )
                         | ( val.second << 14 )
                         | ( int(val.microsecond / 1000 ) <<  4 ) )

        # Add each of the four bytes of the integer
        self._buffer_value += struct.pack( "=i", time_integer )
    # end add_time



    # We need different versions of the following 2 functions for python 2.x vs. 3.x
    # Note: Choosing to have two different definitions even though the difference
    #       is only in one line to avoid excessive python version check per func
    #       call.
    if IS_PYTHON_3:
        def add_timestamp( self, val ):
            """Add a long timestamp to the buffer (can be null)--eight bytes.

            Parameters:
                val (long)
                    Timestamp from the epoch in milliseconds.
            """
            # Longs are eight bytes long
            self.__will_buffer_overflow( _ColumnTypeSize.TIMESTAMP )

            # Handle nulls
            if val is None:
                self._buffer_value += struct.pack( "=q", 0 )
                return
            # end if

            # Encode the timestamp for sharding purposes
            # ------------------------------------------
            # We need to extract the year, month, day, hour etc. fields
            # from the timestamp value PRECISELY the way the server does
            # it; python's datetime deviates every so slightly such that
            # sharding causes a problem.  So, we must use the crazy long
            # calculation below with many constants.
            # Note: Do NOT delete the comments below--they keep your sanity
            #       (nor the commented out lines)
            YEARS_PER_QUAD_YEAR = 4
            DAYS_PER_YEAR       = 365   # not leap year
            DAYS_PER_QUAD_YEAR  = 1461 # (YEARS_PER_QUAD_YEAR * DAYS_PER_YEAR+1)
            DAYS_PER_WEEK       = 7
            HOURS_PER_DAY       = 24
            MINUTES_PER_HOUR    = 60
            SECS_PER_MINUTE     = 60
            MSECS_PER_SEC       = 1000
            MSECS_PER_MINUTE    = 60000 # (MSECS_PER_SEC * SECS_PER_MINUTE)
            MSECS_PER_HOUR      = 3600000 # (MSECS_PER_MINUTE * MINUTES_PER_HOUR)
            MSECS_PER_DAY       = 86400000 # (MSECS_PER_HOUR * HOURS_PER_DAY)
            # MSECS_PER_YEAR      = 31536000000 # (DAYS_PER_YEAR * MSECS_PER_DAY)
            # MSECS_PER_QUAD_YEAR = 126230400000 # (MSECS_PER_DAY * DAYS_PER_QUAD_YEAR)
            YEARS_PER_CENTURY   = 100
            # EPOCH_YEAR          = 1970
            # CENTURIES_PER_QUAD_CENTURY = 4

            # LEAP_DAYS_PER_CENTURY  = 24 # ((YEARS_PER_CENTURY / YEARS_PER_QUAD_YEAR) - 1)
            DAYS_PER_CENTURY       = 36524 # (YEARS_PER_CENTURY * DAYS_PER_YEAR + LEAP_DAYS_PER_CENTURY)
            DAYS_PER_QUAD_CENTURY  = 146097 # (CENTURIES_PER_QUAD_CENTURY * DAYS_PER_CENTURY + 1)
            # MSECS_PER_CENTURY      = 3155673600000 # (DAYS_PER_CENTURY * MSECS_PER_DAY)
            # MSECS_PER_QUAD_CENTURY = 12622780800000  # (DAYS_PER_QUAD_CENTURY * MSECS_PER_DAY)

            # YEARS_TO_EPOCH = 1969 # (EPOCH_YEAR-1) # from year 1
            YEARS_PER_QUAD_CENTURY = 400 # (YEARS_PER_CENTURY*CENTURIES_PER_QUAD_CENTURY)

            # QUAD_CENTURIES_OFFSET          =   4 # (YEARS_TO_EPOCH / YEARS_PER_QUAD_CENTURY)
            # YEAR_IN_QUAD_CENTURY_OFFSET    = 369 # (YEARS_TO_EPOCH % YEARS_PER_QUAD_CENTURY)
            # CENTURY_OF_QUAD_CENTURY_OFFSET =   3 # (YEAR_IN_QUAD_CENTURY_OFFSET / YEARS_PER_CENTURY)
            # YEAR_IN_CENTURY_OFFSET         =  69 # (YEAR_IN_QUAD_CENTURY_OFFSET % YEARS_PER_CENTURY)
            # QUAD_YEAR_OF_CENTURY_OFFSET    =  17 # (YEAR_IN_CENTURY_OFFSET / YEARS_PER_QUAD_YEAR)
            # YEAR_IN_QUAD_YEAR_OFFSET       =   1 # (YEAR_IN_CENTURY_OFFSET % YEARS_PER_QUAD_YEAR)

            # MS_EPOCH_OFFSET = (QUAD_CENTURIES_OFFSET*MSECS_PER_QUAD_CENTURY
            #                    + CENTURY_OF_QUAD_CENTURY_OFFSET*MSECS_PER_CENTURY
            #                    + QUAD_YEAR_OF_CENTURY_OFFSET*MSECS_PER_QUAD_YEAR
            #                    + YEAR_IN_QUAD_YEAR_OFFSET*MSECS_PER_YEAR)
            MS_EPOCH_OFFSET = 62135596800000


            JAN_1_0001_DAY_OF_WEEK = 1  # 0 based day of week - is a friday (as if gregorian calandar started in year 1)

            days_since_1 = (val + MS_EPOCH_OFFSET) // MSECS_PER_DAY
            quad_century = days_since_1  // DAYS_PER_QUAD_CENTURY
            day_of_quad_century = days_since_1 - (quad_century * DAYS_PER_QUAD_CENTURY)
            century_of_quad_century = day_of_quad_century // DAYS_PER_CENTURY
            if (century_of_quad_century == 4):
                century_of_quad_century = 3
            day_of_century = day_of_quad_century - (century_of_quad_century * DAYS_PER_CENTURY)
            quad_year_of_century = day_of_century // DAYS_PER_QUAD_YEAR
            day_of_quad_year = day_of_century - (quad_year_of_century * DAYS_PER_QUAD_YEAR)
            year_of_quad_year = day_of_quad_year // DAYS_PER_YEAR
            if (year_of_quad_year == 4):
                year_of_quad_year = 3

            # We need this extracted value
            day_of_year_field = int( day_of_quad_year - (year_of_quad_year * DAYS_PER_YEAR) + 1 )

            year = (YEARS_PER_QUAD_CENTURY * quad_century) \
                   + (YEARS_PER_CENTURY * century_of_quad_century) \
                   + (YEARS_PER_QUAD_YEAR * quad_year_of_century) \
                   + year_of_quad_year + 1

            # We also need this extracted value
            year_field = int(year - 1900)
            ly = 1 if ((year % YEARS_PER_QUAD_CENTURY) == 0) else \
                 ( 0 if ( (year % YEARS_PER_CENTURY) == 0) else \
                   ( 1 if ((year % YEARS_PER_QUAD_YEAR) == 0) else 0 ) )

            month_of_year_field = None
            dy = day_of_year_field
            if (dy <= 31):
                month_of_year_field = 1
            elif (dy <= (59 + ly )):
                dy -= 31;
                month_of_year_field = 2
            elif (dy <= (90 + ly)):
                dy -= (59 + ly)
                month_of_year_field = 3
            elif (dy <= (120 + ly)):
                dy -= (90 + ly)
                month_of_year_field = 4
            elif (dy <= (151 + ly)):
                dy -= (120 + ly)
                month_of_year_field = 5
            elif (dy <= (181 + ly ) ):
                dy -= (151 + ly)
                month_of_year_field = 6
            elif (dy <= (212 + ly) ):
                dy -= (181 + ly)
                month_of_year_field = 7
            elif (dy <= (243 + ly) ):
                dy -= (212 + ly)
                month_of_year_field = 8
            elif (dy <= (273 + ly) ):
                dy -= (243 + ly)
                month_of_year_field = 9
            elif (dy <= (304 + ly) ):
                dy -= (273 + ly)
                month_of_year_field = 10
            elif (dy <= (334 + ly) ):
                dy -= (304 + ly)
                month_of_year_field = 11
            else:
                dy -= (334 + ly)
                month_of_year_field = 12    # december

            # We need all of the following extracted values
            day_of_month_field = dy
            hour_field   = int(((val + MS_EPOCH_OFFSET) / MSECS_PER_HOUR) % HOURS_PER_DAY)
            minute_field = int(((val + MS_EPOCH_OFFSET) / MSECS_PER_MINUTE) % MINUTES_PER_HOUR)
            sec_field    = int(((val + MS_EPOCH_OFFSET) / MSECS_PER_SEC) % SECS_PER_MINUTE)
            msec_field   = int((val + MS_EPOCH_OFFSET) % MSECS_PER_SEC)
            days_since_0001_from_ms = (val + MS_EPOCH_OFFSET)/ MSECS_PER_DAY
            day_of_week_field = int( ((days_since_0001_from_ms + JAN_1_0001_DAY_OF_WEEK) % DAYS_PER_WEEK) + 1 )

            timestamp = ( (   year_field          << 53 )
                          | ( month_of_year_field << 49 )
                          | ( day_of_month_field  << 44 )
                          | ( hour_field          << 39 )
                          | ( minute_field        << 33 )
                          | ( sec_field           << 27 )
                          | ( msec_field          << 17 )
                          | ( day_of_year_field   <<  8 )
                          | ( day_of_week_field   <<  5 ) )

            # Add the eight bytes of the timestamp (long)
            self._buffer_value += struct.pack( "=q", timestamp )
        # end add_timestamp

    else: # Python 2.x
        def add_timestamp( self, val ):
            """Add a long timestamp to the buffer (can be null)--eight bytes.

            Parameters:
                val (long)
                    Timestamp from the epoch in milliseconds.
            """
            # Longs are eight bytes long
            self.__will_buffer_overflow( _ColumnTypeSize.TIMESTAMP )

            # Handle nulls
            if val is None:
                self._buffer_value += struct.pack( "=q", 0 )
                return
            # end if

            # Encode the timestamp for sharding purposes
            # ------------------------------------------
            # We need to extract the year, month, day, hour etc. fields
            # from the timestamp value PRECISELY the way the server does
            # it; python's datetime deviates every so slightly such that
            # sharding causes a problem.  So, we must use the crazy long
            # calculation below with many constants.
            # Note: Do NOT delete the comments below--they keep your sanity
            YEARS_PER_QUAD_YEAR = 4
            DAYS_PER_YEAR       = 365   # not leap year
            DAYS_PER_QUAD_YEAR  = 1461 # (YEARS_PER_QUAD_YEAR * DAYS_PER_YEAR+1)
            DAYS_PER_WEEK       = 7
            HOURS_PER_DAY       = 24
            MINUTES_PER_HOUR    = 60
            SECS_PER_MINUTE     = 60
            MSECS_PER_SEC       = 1000
            MSECS_PER_MINUTE    = 60000 # (MSECS_PER_SEC * SECS_PER_MINUTE)
            MSECS_PER_HOUR      = 3600000 # (MSECS_PER_MINUTE * MINUTES_PER_HOUR)
            MSECS_PER_DAY       = 86400000 # (MSECS_PER_HOUR * HOURS_PER_DAY)
            # MSECS_PER_YEAR      = 31536000000 # (DAYS_PER_YEAR * MSECS_PER_DAY)
            # MSECS_PER_QUAD_YEAR = 126230400000 # (MSECS_PER_DAY * DAYS_PER_QUAD_YEAR)
            YEARS_PER_CENTURY   = 100
            # EPOCH_YEAR          = 1970
            # CENTURIES_PER_QUAD_CENTURY = 4

            # LEAP_DAYS_PER_CENTURY  = 24 # ((YEARS_PER_CENTURY / YEARS_PER_QUAD_YEAR) - 1)
            DAYS_PER_CENTURY       = 36524 # (YEARS_PER_CENTURY * DAYS_PER_YEAR + LEAP_DAYS_PER_CENTURY)
            DAYS_PER_QUAD_CENTURY  = 146097 # (CENTURIES_PER_QUAD_CENTURY * DAYS_PER_CENTURY + 1)
            # MSECS_PER_CENTURY      = 3155673600000 # (DAYS_PER_CENTURY * MSECS_PER_DAY)
            # MSECS_PER_QUAD_CENTURY = 12622780800000  # (DAYS_PER_QUAD_CENTURY * MSECS_PER_DAY)

            # YEARS_TO_EPOCH = 1969 # (EPOCH_YEAR-1) # from year 1
            YEARS_PER_QUAD_CENTURY = 400 # (YEARS_PER_CENTURY*CENTURIES_PER_QUAD_CENTURY)

            # QUAD_CENTURIES_OFFSET          =   4 # (YEARS_TO_EPOCH / YEARS_PER_QUAD_CENTURY)
            # YEAR_IN_QUAD_CENTURY_OFFSET    = 369 # (YEARS_TO_EPOCH % YEARS_PER_QUAD_CENTURY)
            # CENTURY_OF_QUAD_CENTURY_OFFSET =   3 # (YEAR_IN_QUAD_CENTURY_OFFSET / YEARS_PER_CENTURY)
            # YEAR_IN_CENTURY_OFFSET         =  69 # (YEAR_IN_QUAD_CENTURY_OFFSET % YEARS_PER_CENTURY)
            # QUAD_YEAR_OF_CENTURY_OFFSET    =  17 # (YEAR_IN_CENTURY_OFFSET / YEARS_PER_QUAD_YEAR)
            # YEAR_IN_QUAD_YEAR_OFFSET       =   1 # (YEAR_IN_CENTURY_OFFSET % YEARS_PER_QUAD_YEAR)

            # MS_EPOCH_OFFSET = (QUAD_CENTURIES_OFFSET*MSECS_PER_QUAD_CENTURY
            #                    + CENTURY_OF_QUAD_CENTURY_OFFSET*MSECS_PER_CENTURY
            #                    + QUAD_YEAR_OF_CENTURY_OFFSET*MSECS_PER_QUAD_YEAR
            #                    + YEAR_IN_QUAD_YEAR_OFFSET*MSECS_PER_YEAR)
            MS_EPOCH_OFFSET = 62135596800000


            JAN_1_0001_DAY_OF_WEEK = 1  # 0 based day of week - is a friday (as if gregorian calandar started in year 1)

            days_since_1 = (val + MS_EPOCH_OFFSET) / MSECS_PER_DAY
            quad_century = days_since_1  /DAYS_PER_QUAD_CENTURY
            day_of_quad_century = days_since_1 - (quad_century * DAYS_PER_QUAD_CENTURY)
            century_of_quad_century = day_of_quad_century / DAYS_PER_CENTURY
            if (century_of_quad_century == 4):
                century_of_quad_century = 3
            day_of_century = day_of_quad_century - (century_of_quad_century * DAYS_PER_CENTURY)
            quad_year_of_century = day_of_century / DAYS_PER_QUAD_YEAR
            day_of_quad_year = day_of_century - (quad_year_of_century * DAYS_PER_QUAD_YEAR)
            year_of_quad_year = day_of_quad_year / DAYS_PER_YEAR
            if (year_of_quad_year == 4):
                year_of_quad_year = 3

            # We need this extracted value
            day_of_year_field = int( day_of_quad_year - (year_of_quad_year * DAYS_PER_YEAR) + 1 )

            year = (YEARS_PER_QUAD_CENTURY * quad_century) \
                   + (YEARS_PER_CENTURY * century_of_quad_century) \
                   + (YEARS_PER_QUAD_YEAR * quad_year_of_century) \
                   + year_of_quad_year + 1

            # We also need this extracted value
            year_field = int(year - 1900)
            ly = 1 if ((year % YEARS_PER_QUAD_CENTURY) == 0) else \
                 ( 0 if ( (year % YEARS_PER_CENTURY) == 0) else \
                   ( 1 if ((year % YEARS_PER_QUAD_YEAR) == 0) else 0 ) )

            month_of_year_field = None
            dy = day_of_year_field
            if (dy <= 31):
                month_of_year_field = 1
            elif (dy <= (59 + ly )):
                dy -= 31;
                month_of_year_field = 2
            elif (dy <= (90 + ly)):
                dy -= (59 + ly)
                month_of_year_field = 3
            elif (dy <= (120 + ly)):
                dy -= (90 + ly)
                month_of_year_field = 4
            elif (dy <= (151 + ly)):
                dy -= (120 + ly)
                month_of_year_field = 5
            elif (dy <= (181 + ly ) ):
                dy -= (151 + ly)
                month_of_year_field = 6
            elif (dy <= (212 + ly) ):
                dy -= (181 + ly)
                month_of_year_field = 7
            elif (dy <= (243 + ly) ):
                dy -= (212 + ly)
                month_of_year_field = 8
            elif (dy <= (273 + ly) ):
                dy -= (243 + ly)
                month_of_year_field = 9
            elif (dy <= (304 + ly) ):
                dy -= (273 + ly)
                month_of_year_field = 10
            elif (dy <= (334 + ly) ):
                dy -= (304 + ly)
                month_of_year_field = 11
            else:
                dy -= (334 + ly)
                month_of_year_field = 12    # december

            # We need all of the following extracted values
            day_of_month_field = dy
            hour_field   = int(((val + MS_EPOCH_OFFSET) / MSECS_PER_HOUR) % HOURS_PER_DAY)
            minute_field = int(((val + MS_EPOCH_OFFSET) / MSECS_PER_MINUTE) % MINUTES_PER_HOUR)
            sec_field    = int(((val + MS_EPOCH_OFFSET) / MSECS_PER_SEC) % SECS_PER_MINUTE)
            msec_field   = int((val + MS_EPOCH_OFFSET) % MSECS_PER_SEC)
            days_since_0001_from_ms = (val + MS_EPOCH_OFFSET)/ MSECS_PER_DAY
            day_of_week_field = int( ((days_since_0001_from_ms + JAN_1_0001_DAY_OF_WEEK) % DAYS_PER_WEEK) + 1 )

            timestamp = ( (   year_field          << 53 )
                          | ( month_of_year_field << 49 )
                          | ( day_of_month_field  << 44 )
                          | ( hour_field          << 39 )
                          | ( minute_field        << 33 )
                          | ( sec_field           << 27 )
                          | ( msec_field          << 17 )
                          | ( day_of_year_field   <<  8 )
                          | ( day_of_week_field   <<  5 ) )

            # Add the eight bytes of the timestamp (long)
            self._buffer_value += struct.pack( "=q", timestamp )
        # end add_timestamp

    # end defining python version specific add_timestamp()


    @staticmethod
    def is_unsigned_long( value ):
        """Check if the given value is an unsigned long.  If parsable as
        as unsigned long, return the value; else, return False.  Note
        that it returns different types of things based on the parsing.
        """
        # Length of the maximum unsigned long value
        max_len = 20

        str_len = len( value )
        if ( (str_len == 0) or (str_len > max_len) ):
            return False

        # Parse the value as a long
        try:
            ulong_value = builtins.int( value )
        except ValueError as e:
            return False

        # Make sure it's within the 64-bit unsigned long range
        if ( (ulong_value < 0) or (ulong_value > 18446744073709551615) ):
            return False

        return ulong_value
    # end is_unsigned_long


    def add_ulong( self, val ):
        """Add an unsigned long to the buffer (can be null)--eight bytes.
        Given value is a string; need to parse.  If not a valid unsigned
        long value, throw an exception.

        @throws GPUdbException if the value cannot be parsed as unsigned long
        """
        # Longs are eight bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.ULONG )

        # Handle nulls
        if val is None:
            self._buffer_value += struct.pack( "=q", 0 )
            return
        # end if

        ulong_value = _RecordKey.is_unsigned_long( val )
        # Make sure that zero does not get falsely evaluated
        if ( isinstance(ulong_value, bool) and (ulong_value == False) ):
            raise GPUdbException( "Value '{}' could not be parsed as an unsigned"
                                  " long!".format( val ) )

        # Add the eight bytes of the unsigned long
        self._buffer_value += struct.pack( "=Q", ulong_value )
    # end add_ulong



    @staticmethod
    def validate_uuid( value ):
        """Check if the given value is a UUID.  If parsable as
        as UUID, return the value; else, return False.
        """
        #  From core/Utils/Uuid.cpp:
        #  Accept 'xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx', with or without
        #  hyphens are fine.
        #  Version Msb0  Msb1  Msb2 Msb3     Description
        #  1       0     0     0    1        The time-based version specified
        #                                    in this document.
        #  2       0     0     1    0        DCE Security version, with embedded
        #                                    POSIX UIDs.
        #  3       0     0     1    1        The name-based version specified in
        #                                    this document that uses MD5 hashing.
        #  4       0     1     0    0        The randomly or pseudo- randomly
        #                                    generated version specified in this
        #                                    document.
        #  5       0     1     0    1        The name-based version specified in
        #                                    this document that uses SHA-1
        #                                    hashing.

        # Check that it is a string!
        if not isinstance(value, (basestring, unicode)):
            mh_log_debug( "Given UUID value {} is not a string!".format( value ) )
            return False
        # end if

        # Validation based on string length
        str_len = len( value )
        if (str_len == 36):
            has_hyphens = True
        elif (str_len == 32):
            has_hyphens = False
        else:
            # We have only two possible lengths for UUIDs: 36 & 32
            return False
        # end if

        # Parse each character to validate the content of the value
        for (idx, c) in enumerate( value ):
            if has_hyphens:
                if ( ( (idx == 8) or (idx == 13) or (idx == 18) or (idx == 23) )
                     and (c != '-') ):
                    # Supposed to be a hyphen!
                    return False
                elif (idx == 14):
                    # TODO: Figure out this logic; don't understand this!
                    if ((c < '1') or (c > '5')):
                        return False
                # end if

            else:
                # If not a hyphen, it better be a digit!
                if not c.isdigit():
                    return False
                # end if

                # TODO: Figure out this logic, too!
                if ( (idx == 12) and ((c < '1') or (c > '5')) ):
                    return False
                # end if
            # end if
        # end for

        # Extract any hyphen from the UUID and return just the digits
        if has_hyphens:
            return value.replace( '-', '' )
        else:
            # Nothing to extract!
            return value
        # end if
    # end validate_uuid


    def add_uuid( self, val ):
        """Add a UUID to the buffer (can be null)--16 bytes (128 bits).
        Given value is a string; need to parse.  If not a valid UUID,
        throw an exception.

        @throws GPUdbException if the value cannot be parsed as a UUID
        """
        # Longs are eight bytes long
        self.__will_buffer_overflow( _ColumnTypeSize.UUID )

        # Handle nulls
        if val is None:
            # Add 16 0s
            for i in list( range( 0, _ColumnTypeSize.UUID ) ):
                self._buffer_value += struct.pack( "=b", 0 )
            # end for
            return
            # self._buffer_value += struct.pack( "=q", 0 )
            # return
        # end if

        # Check that it is indeed a valid UUID (this will also extract
        # the hyphens and return just th hexadecimal digits if it is a valid
        # UUID)
        parsed_uuid = _RecordKey.validate_uuid( val )
        if (parsed_uuid is False):
            # The validating function returns False if it is an invalid UUID
            raise GPUdbException( "Value '{}' could not be parsed as a UUID!"
                                  "".format( val ) )
        # end if

        def convert_hex_to_int( hex_digit ):
            """Internal helper method to convert a hexadecimal digit to integer.
            """
            if hex_digit.isdigit():
                # We just need to numerical value
                return int( hex_digit )
            # if ( (hex_digit >= '0') and (hex_digit <= '9') ):
            #     return (hex_digit - '0')
            elif ( (hex_digit >= 'A') and (hex_digit <= 'F') ):
                return (ord(hex_digit) - ord('A') + 10)
            elif ( (hex_digit >= 'a') and (hex_digit <= 'f') ):
                return (ord(hex_digit) - ord('a') + 10)
                # return (hex_digit - 'a' + 10)
            else:
                raise GPUdbException( "Unknown hexadecimal value given ({})!"
                                      "".format( hex_digit ) )
        # end convert_hex_to_int


        # Parse the UUID segments and store in a little-endian fashion
        for i in range(15, -1, -1):
            # Iterate over 15 to 0, decrementing by one, to store the values
            # in a little-endian fashion
            byte_val = ( (convert_hex_to_int( parsed_uuid[ 2 * i ] ) << 4)
                         + convert_hex_to_int( parsed_uuid[ 2 * i + 1] ) )
            byte_val = byte_val & 0xFF
            self._buffer_value += struct.pack( "B", byte_val )
        # end for
    # end add_uuid




    # We need different versions of the following 2 functions for python 2.x vs. 3.x
    # Note: Choosing to have two different definitions even though the difference
    #       is only in one line to avoid excessive python version check per func
    #       call.
    if IS_PYTHON_3:
        def compute_hashes( self ):
            """Compute the Murmur hash of the key.
            """
            a = mmh3.hash64( self._buffer_value, seed = 10 )
            self._routing_hash = a[ 0 ] # the first half

            self._hash_code = int( self._routing_hash ^ ( self._routing_hash >> 32 ) )
        # end compute_hashes

    else: # Python 2.x
        def compute_hashes( self ):
            """Compute the Murmur hash of the key.
            """
            a = mmh3.hash64( str( self._buffer_value ), seed = 10 )
            self._routing_hash = a[ 0 ] # the first half

            self._hash_code = int( self._routing_hash ^ ( self._routing_hash >> 32 ) )
        # end compute_hashes
    # end python version dependent definition


    def route( self, routing_table ):
        """Given a routing table, return the rank of the GPUdb server that
        this record key should be routed to.

        @param routing_table  A list of integers...

        @returns the rank of the GPUdb server that this record key should be
                 routed to.
        """
        if not routing_table: # no routing info is provided
            return 0

        routing_table_len = len( routing_table )
        routing_index = (abs( self._routing_hash ) % routing_table_len )

        if (routing_index >= routing_table_len ):
            raise GPUdbException( "Computed routing index ({ind}) is out-of-bounds "
                                  "(table length {l})"
                                  "".format( ind = routing_index,
                                             l   = routing_table_len ) )

        # Return the nth element of routing_table where
        #    n == (record key hash) % (number of elements in routing_table)
        return routing_table[ routing_index ]
    # end route
# end class _RecordKey



# Internal Class _RecordKeyBuilder
# ================================
class _RecordKeyBuilder:
    """Creates RecordKey objects given a particular kind of table schema.
    """

    # A dict mapping column type to _RecordKey appropriate add functions
    _column_type_add_functions = collections.OrderedDict()
    _column_type_add_functions[ "char1"     ] = _RecordKey.add_char1
    _column_type_add_functions[ "char2"     ] = _RecordKey.add_char2
    _column_type_add_functions[ "char4"     ] = _RecordKey.add_char4
    _column_type_add_functions[ "char8"     ] = _RecordKey.add_char8
    _column_type_add_functions[ "char16"    ] = _RecordKey.add_char16
    _column_type_add_functions[ "char32"    ] = _RecordKey.add_char32
    _column_type_add_functions[ "char64"    ] = _RecordKey.add_char64
    _column_type_add_functions[ "char128"   ] = _RecordKey.add_char128
    _column_type_add_functions[ "char256"   ] = _RecordKey.add_char256
    _column_type_add_functions[ "date"      ] = _RecordKey.add_date
    _column_type_add_functions[ "datetime"  ] = _RecordKey.add_datetime
    _column_type_add_functions[ "double"    ] = _RecordKey.add_double
    _column_type_add_functions[ "float"     ] = _RecordKey.add_float
    _column_type_add_functions[ "int"       ] = _RecordKey.add_int
    _column_type_add_functions[ "int8"      ] = _RecordKey.add_int8
    _column_type_add_functions[ "int16"     ] = _RecordKey.add_int16
    _column_type_add_functions[ "long"      ] = _RecordKey.add_long
    _column_type_add_functions[ "string"    ] = _RecordKey.add_string
    _column_type_add_functions[ "decimal"   ] = _RecordKey.add_decimal
    _column_type_add_functions[ "ipv4"      ] = _RecordKey.add_ipv4
    _column_type_add_functions[ "time"      ] = _RecordKey.add_time
    _column_type_add_functions[ "timestamp" ] = _RecordKey.add_timestamp
    _column_type_add_functions[ "ulong"     ] = _RecordKey.add_ulong
    _column_type_add_functions[ "uuid"      ] = _RecordKey.add_uuid


    # A dict for string types
    _string_types = [ "char1",  "char2",  "char4",  "char8",
                      "char16", "char32", "char64", "char128", "char256",
                      "date", "datetime", "decimal", "ipv4", "time",
                      "uuid", "string" ]

    def __init__( self, record_type,
                  is_primary_key = False ):
        """Initializes a RecordKeyBuilder object.
        """
        # Check the input parameter type 'record_type'
        if not isinstance(record_type, GPUdbRecordType):
            raise GPUdbException("Parameter 'record_type' must be of type "
                                 "GPUdbRecordType; given %s" % str( type( record_type ) ) )

        # Validate the boolean parameters
        if is_primary_key not in [True, False]:
            raise GPUdbException( "Constructor parameter 'is_primary_key' must be a "
                                  "boolean value; given: %s" % is_primary_key )

        # Save the record schema related information
        self._record_type         = record_type
        self._record_column_names = record_type.column_names
        self._column_properties   = record_type.column_properties

        # A list of which columns are primary/shard keys
        self.routing_key_indices = []
        self.key_columns_names = []
        self.key_schema_fields = []
        self.key_schema_str = None
        self._key_types = []

        # Go over all columns and see which ones are primary or shard keys
        for i in list( range(0, len( record_type.columns )) ):
            column_name = self._record_column_names[ i ]
            column_type = record_type.columns[ i ].column_type
            column_properties = self._column_properties[ column_name ] \
                                if (column_name in self._column_properties) else None

            is_key = False
            # Check for primary keys, if any
            if is_primary_key and column_properties and (C._pk in column_properties):
                is_key = True
            elif ( (not is_primary_key)
                   and column_properties and (C._shard_key in column_properties) ):
                # turned out to be a shard key
                is_key = True

            # Save the key index for primary or shard keys
            if is_key:
                self.routing_key_indices.append( i )
                self.key_columns_names.append( column_name )

                # Build the key schema fields
                key = collections.OrderedDict()
                key[ C._name ] = column_name
                key[ C._type ] = column_type
                key[ C._is_nullable ] = (GPUdbColumnProperty.NULLABLE in column_properties)
                self.key_schema_fields.append( key )
            # end if
        # end loop over columns

        # Check if it's a track-type
        track_type_special_columns = set(["TRACKID", "TIMESTAMP", "x", "y"])
        is_track_type = track_type_special_columns.issubset( self._record_column_names )
        if ((not is_primary_key) and is_track_type):
            track_id_index = self._record_column_names.index( "TRACKID" )
            if not self.routing_key_indices: # no pk/shard key found yet
                self.routing_key_indices.append( track_id_index )

                # Add the track ID to the schema fields for the keys
                key = collections.OrderedDict()
                key[ C._name ] = column_name
                key[ C._type ] = column_type
                self.key_schema_fields.append( key )
            elif ( (len( self.routing_key_indices ) != 1)
                   or (self.routing_key_indices[0] != track_id_index ) ):
                raise GPUdbException( "Cannot have a shard key other than "
                                      "'TRACKID' for track-type tables." )
        # end checking track-type tables


        self._key_buffer_size = 0
        if not self.routing_key_indices: # no pk/shard key found
            return None
        # end if


        # Calculate the buffer size for this type of objects/records
        # with the given primary (and/or) shard keys
        for i in self.routing_key_indices:
            column_name = self._record_column_names[ i ]
            column_type = record_type.columns[ i ].column_type
            column_properties = self._column_properties[ column_name ] \
                                if (column_name in self._column_properties) else None

            # Check for any property related to data types
            type_related_properties = set( column_properties ).intersection( _ColumnTypeSize.column_type_sizes.keys() )
            type_related_properties = list( type_related_properties )

            # Process any special property related to the data type
            if type_related_properties:
                # Check that only one type-related property found
                if (len(type_related_properties) > 1):
                    raise GPUdbException( "Column '%s' has multiple type-related properties "
                                          "(can have at most one): %s"
                                          "" % (column_name, str( type_related_properties ) ) )
                # Use the special property and its size for the data type
                column_type = type_related_properties[ 0 ]
            # end if

            # Increment the key's buffer size and save the column type
            self._key_buffer_size += _ColumnTypeSize.column_type_sizes[ column_type ]
            self._key_types.append( column_type )
        # end loop


        # Build the key schema
        key_schema_fields_str = []
        for key in self.key_schema_fields:
            key_name = key[ C._name ]
            key_type = key[ C._type ]
            if key[ C._is_nullable ]:
                key_type = '["{_t}", "null"]'.format( _t = key_type )
            else:
                key_type = '"{_t}"'.format( _t = key_type )
            key_field_description = ( '{{"name":"{key}", "type":{_t}}}'
                                      ''.format( key = key_name,
                                                 _t  = key_type ) )
            key_schema_fields_str.append( key_field_description )
        # end loop

        key_schema_fields_str = ",".join( key_schema_fields_str )
        key_schema_fields_str = key_schema_fields_str.replace(" ", "").replace("\n","")
        self.key_schema_str = ("""{ "type" : "record",
                                   "name" : "key_schema",
                                   "fields" : [%s] }""" \
                                       % key_schema_fields_str )
        self.key_schema_str = self.key_schema_str.replace(" ", "").replace("\n","")
        self.key_schema = schema.parse( self.key_schema_str )
    # end RecordKeyBuilder __init__



    def build( self, record ):
        """Builds a RecordKey object based on the input data and returns it.

        Parameters:

            record (OrderedDict or GPUdbRecord)
                The object based on which the key is to be built.

        Returns:
            A _RecordKey object.
        """
        # Nothing to do if the key size is zero!
        if (self._key_buffer_size == 0):
            return None

        # Extract the internal ordered dict if it's a GPUdbRecord
        if isinstance( record, GPUdbRecord ):
            record = record.column_values
        # end if

        # Check that we got a valid record by size
        if isinstance( record, (dict, GPUdbRecord, Record,
                                collections.OrderedDict) ):
            # Got a dict-compatible object; make sure we have the correct
            # number of columns (need to explicitly convert to a list for
            # python 3)
            record_keys = list( record.keys() )
            if ( record_keys != self._record_column_names):
                raise GPUdbException( "Given record must be of the type '{}'"
                                      " (with columns {}); given record has columns: {} "
                                      "".format( self._record_type.schema_string,
                                                 self._record_column_names,
                                                 record_keys ) )
            # end if

            # Need to explicitly convert to a list for python 3
            column_values = list( record.values() )
        elif isinstance( record, list ):
            # Got a dict-compatible object; make sure we have the correct
            # number of columns
            num_columns = len(record)
            if ( num_columns != len(self._record_column_names)):
                raise GPUdbException( "Given record must be of the type '{}'"
                                      " (with columns {}); got a list of {}"
                                      " columns"
                                      "".format( self._record_type.schema_string,
                                                 self._record_column_names,
                                                 num_columns ) )
            # end if

            column_values = record
        else:
            # We need to at least have a
            raise GPUdbException( "Give record must be a dict-compatible object "
                                  "(dict, OrderedDict, GPUdbRecord, Record) or "
                                  "a list; got {}".format( str(type( record )) ) )

        # Create and populate a RecordKey object
        record_key = _RecordKey( self._key_buffer_size )

        # Add each routing column's value to the key
        for i, key_idx in enumerate( self.routing_key_indices ):
            # Extract the value for the relevant routing column
            value = column_values[ key_idx ]

            # Based on the column's type, call the appropriate
            # Record.add_xxx() function
            col_type = self._key_types[ i ]
            self._column_type_add_functions[ col_type ]( record_key, value )
        # end loop

        # Compute the key hash and return the key
        record_key.compute_hashes()
        return record_key
    # end build()



    def build_key_with_shard_values_only( self, key_values ):
        """Builds a RecordKey object based on the input data and returns it.

        Parameters:

            key_values (list or dict)
                Values for the sharding columns either in a list (then is
                assumed to be in the order of the sharding keys in the record
                type) or a dict.  Must not have any missing key value or any
                extra column values.

        Returns:
            A _RecordKey object.
        """
        # Nothing to do if the key size is zero!
        if (self._key_buffer_size == 0):
            return None

        # Type checking
        if ( (not isinstance(key_values, list))
             and (not isinstance(key_values, dict)) ):
            raise GPUdbException( "Argument 'key_values' must be either a list "
                                  "or a dict; given %s" % str(type( key_values )))

        # Make sure that there are the correct number of values given
        if ( len( key_values ) != len( self.key_columns_names ) ):
            raise GPUdbException( "Incorrect number of key values specified; expected "
                                  " %d, received %d" % ( len( self.key_columns_names ),
                                                         len( key_values ) ) )

        # If a dict is given, convert it into a list in the order of the key columns
        if isinstance( key_values, dict ):
            try:
                key_values = [ key_values[ _name ] for _name in self.key_columns_names  ]
            except KeyError as missing_key:
                # Did not find a column in the given values
                raise GPUdbException( "Missing value for column '%s' in input argument "
                                      "'key_values'" % missing_key)
        # end if


        # Create and populate a RecordKey object
        record_key = _RecordKey( self._key_buffer_size )

        # Add each routing column's value to the key
        for i in list( range( 0, len( self.routing_key_indices ) ) ):
            # Extract the value for the relevant routing column
            value = key_values[ i ]

            # Based on the column's type, call the appropriate
            # Record.add_xxx() function
            col_type = self._key_types[ i ]
            self._column_type_add_functions[ col_type ]( record_key, value )
        # end loop

        # Compute the key hash and return the key
        record_key.compute_hashes()
        return record_key
    # end build_key_with_shard_values_only



    def build_expression_with_key_values_only( self, key_values ):
        """Builds an expressiong of the format "(x = 1) and is_null(y) and ..."
        where the column names would be the key's column names, and the values
        would be key's values, using the function 'is_null()' for null values.

        Parameters:

            key_values (list or dict)
                Values for the sharding columns either in a list (then is
                assumed to be in the order of the sharding keys in the record
                type) or a dict.  Must not have any missing key value or any
                extra column values.

        Returns:
            A string with the expression built based on the input values.
        """
        # Nothing to do if the key size is zero!
        if (self._key_buffer_size == 0):
            return None

        # Type checking
        if ( (not isinstance(key_values, list))
             and (not isinstance(key_values, dict)) ):
            raise GPUdbException( "Argument 'key_values' must be either a list "
                                  "or a dict; given %s" % str(type( key_values )))

        # Make sure that there are the correct number of values given
        if ( len( key_values ) != len( self.key_columns_names ) ):
            raise GPUdbException( "Incorrect number of key values specified; expected "
                                  " %d, received %d" % ( len( self.key_columns_names),
                                                         len( key_values ) ) )

        # If a dict is given, convert it into a list in the order of the key columns
        if isinstance( key_values, dict ):
            try:
                key_values = [ key_values[ _name ] for _name in self.key_columns_names  ]
            except KeyError as missing_key:
                # Did not find a column in the given values
                raise GPUdbException( "Missing value for column '%s' in input argument "
                                      "'key_values'" % missing_key)
        # end if

        # Generate the expression predicates per column
        predicates = []
        for i in list( range( 0, len( self.routing_key_indices ) ) ):
            # Extract the value for the relevant routing column
            key_value = key_values[ i ]
            col_type = self._key_types[ i ]
            col_name = self.key_columns_names[ i ]

            # Handle unsigned longs specially (only when it's not a null)
            if ( (col_type == "ulong") and (key_value is not None) ):
                ulong_value = _RecordKey.is_unsigned_long( key_value )
                # Make sure that zero does not get falsely evaluated
                if ( isinstance(ulong_value, bool) and (ulong_value == False) ):
                    raise GPUdbException( "Value '{}' could not be parsed as an unsigned"
                                          " long!".format( key_value ) )

                key_value = ulong_value
            # end if

            # Add the column's value (use function 'is_null()' if the value is a null,
            # otherwise just an equivalency, with double quotes for string types)
            if (key_value is None):
                # Handle nulls specially
                predicate = "is_null({n})".format( n = col_name)
            elif (col_type in self._string_types):
                # String values need to be quoted
                predicate = '({n} = "{d}")'.format( n = col_name,
                                                    d = key_value )
            else:
                predicate = '({n} = {d})'.format( n = col_name,
                                                  d = key_value )

            predicates.append( predicate )
        # end loop

        # Put them together to form the overall expression
        expression = " and ".join( predicates )

        return expression
    # end build_expression_with_key_values_only



    def has_key( self ):
        """Checks whether this record has any key associated with it.
        """
        return (len( self.key_columns_names ) > 0)
    # end has_key


    def has_same_key( self, other_record_key_builder ):
        """Checks if the given record key builder is equivalent
        to this one.
        """
        return (self.key_schema_str == other_record_key_builder.key_schema_str)
    # end has_same_key


    def build_expression_with_dict( self, values_dict, extra_expression = "" ):
        """Builds an expressiong of the format "(x = 1) and is_null(y) and ..."
        where the column names would be the key's column names, and the values
        would be key's values, using the function 'is_null()' for null values,
        based on the given dict.

        Parameters:

            values_dict (dict)
                Values for the sharding columns in a dict.  Does not do any
                check against any table's type; i.e. absolutely no error checking
                is done on the column names or the columns types or values.
            extra_expression (str)
                Any additional expression; default is an empty string

        Returns:
            A string with the expression built based on the input values.
        """
        if not isinstance( values_dict, dict ):
            raise GPUdbException( "Must provide a dict, given '{}'"
                                  "".format( str(type(values_dict)) ) )

        # Build an expression with the given values, but take care of nulls
        expression_items = []
        for key, value in values_dict.items():
            # Ensure that there is a column with the given name
            col_name = key
            if ( col_name not in self._record_column_names ):
                raise GPUdbException( "No column with name with given key "
                                      "'{}' exists in the type"
                                      "".format( col_name ) )

            # Get the column's type
            col_type = self._record_type.get_column( col_name ).column_type

            # Generate the predicate based on the column value and/or type
            if col_name is None:
                # Handle nulls specially
                predicate = "is_null({})".format( col_name )
            elif (col_type in self._string_types):
                # String values need to be quoted
                predicate = '({n} = "{d}")'.format( n = col_name,
                                                    d = value )
            else:
                predicate = '({n} = {d})'.format( n = col_name,
                                                  d = value )

            # Add the predicate to the list of expressions to be used
            expression_items.append( predicate )
            # expression_items.append( "({} == {})".format( key, val ) )
        # end loop

        # Put the expression together
        expression = " and ".join( expression_items )

        if extra_expression:
            expression = "({}) and ({})".format( expression, extra_expression )

        return expression
    # end build_expression_with_dict

# end class _RecordKeyBuilder #########################



# Internal Class _WorkerQueue
# ===========================
class _WorkerQueue:
    """Maintains a queue for the worker nodes/ranks of the GPUdb server.
    """

    def __init__( self,
                  url = "127.0.0.1:9191",
                  capacity = 10000,
                  has_primary_key = False,
                  update_on_existing_pk = False ):
        """Creates an insertion queue for a given worker rank.
        """
        # Validate the capacity
        if (capacity <= 0):
            raise GPUdbException( "Constructor parameter 'capacity' must be a"
                                  "non-zero positive value; given: %d" % capacity )
        # Validate the boolean parameters
        if has_primary_key not in [True, False]:
            raise GPUdbException( "Constructor parameter 'has_primary_key' must be a "
                                  "boolean value; given: %s" % has_primary_key )
        if update_on_existing_pk not in [True, False]:
            raise GPUdbException( "Constructor parameter 'update_on_existing_pk' must be a "
                                  "boolean value; given: %s" % update_on_existing_pk )

        url = str( url ) # in case it's unicode

        # Save the values
        self.url = url
        self.capacity = capacity
        self.has_primary_key = has_primary_key
        self.update_on_existing_pk = update_on_existing_pk

        # Initialize other members:
        # A queue for the data
        self.record_queue = []

        # A map of pk/shard key to queue index for that data
        # (if the table contains primary keys)
        self.primary_key_to_queue_index_map = None
        if self.has_primary_key:
            self.primary_key_to_queue_index_map = {}
    # end WorkerQueue __init__



    def insert( self, record, key ):
        """Insert a record into the queue (if it checks out).  Return
        the queue if it becomes full afterward.

        Parameters:
            record (GPUdbRecord or OrderedDict)
                The record to be inserted.

            key (_RecordKey)
                A primary key, if any.

        Returns:
            The list of records (if the queue becomes full) or None.
        """
        old_queue_length = len( self.record_queue )

        # Need to check a lot of stuff if the record has a valid primary key
        if (self.has_primary_key and key.is_valid):
            key_hash_code = key.hash_code

            if self.update_on_existing_pk:
                # Update on existing primary key (if key exists)
                if key_hash_code in self.primary_key_to_queue_index_map:
                    # Find the index for this key in the record queue
                    key_index = self.primary_key_to_queue_index_map[ key_hash_code ]
                    self.record_queue[ key_index ] = record
                else: # key does NOT exist
                    # Add the key to the queue and keep track of the queue
                    # index in the key->index map
                    self.record_queue.append( record )
                    self.primary_key_to_queue_index_map[ key_hash_code ] = old_queue_length
                # end inner if
            else: # if key already exists, do NOT insert this record
                if key_hash_code in self.primary_key_to_queue_index_map:
                    # Yes, the key exists, so, it's a problem
                    return None
                else: # key does not already exist
                    self.record_queue.append( record )
                    self.primary_key_to_queue_index_map[ key_hash_code ] = old_queue_length
            # end update on existing PK if-else
        else:
            # The table has no primary key; so no map to worry about
            self.record_queue.append( record )
        # end outer if-else

        # Flush the record queue when full capacity has been reached
        if (len( self.record_queue ) == self.capacity):
            # Return whatever flush returns (which is the current/old queue)
            return self.flush()
        else:
            # return none to indicate nothing to do
            return None
    # end insert



    def flush( self ):
        """Return the current (old) record queue and create a new empty one.

        Returns:
            A list of records to be inserted.
        """
        old_queue = self.record_queue

        # Create a fresh new queue
        self.record_queue = []

        # if a key->record_queue_index map exists, clear it
        if self.primary_key_to_queue_index_map:
            self.primary_key_to_queue_index_map = {}

        return old_queue
    # end flush


    def get_url( self ):
        """Return the URL."""
        return self.url
    # end get_url

# end class _WorkerQueue




class GPUdbIngestor:

    # The default number of times insertions will be re-attempted
    __DEFAULT_INSERTION_RETRY_COUNT = 1


    def __init__( self,
                  gpudb,
                  table_name,
                  record_type,
                  batch_size,
                  options = None,
                  workers = None,
                  is_table_replicated = False ):
        """Initializes the GPUdbIngestor instance.

        Parameters:
            gpudb (GPUdb)
                The client handle through which the ingestion process
                is to be conducted.
            table_name (str)
                The name of the table into which records will be ingested.
                Must be an existing table.
            record_type (GPUdbRecordType)
                The type for the records which will be ingested; must match
                the type of the given table.
            batch_size (int)
                The size of the queues; when any queue (one per worker rank of the
                database server) attains the given size, the queued records
                will be automatically flushed.  Until then, those records will
                be held client-side and not actually ingested.  (Unless
                :meth:`.flush` is called, of course.)
            options (dict of str to str)
                Any insertion options to be passed onto the GPUdb server.  Optional
                parameter.
            workers (GPUdbWorkerList)
                Optional parameter.  A list of GPUdb worker rank addresses.
            is_table_replicated (bool)
                Optional boolean flag indicating whether the table is replicated; if
                True, then multi-head ingestion will not be used (but the head node
                would be used for ingestion instead).  This is due to GPUdb not
                supporting multi-head ingestion on replicated tables.
        """

        # Validate input parameter 'gpudb'
        if not isinstance(gpudb, GPUdb):
            raise GPUdbException( "Parameter 'gpudb' must be of "
                                  "type GPUdb; given %s"
                                  % str( type( gpudb ) ) )
        # Validate input parameter 'table_name'
        if not isinstance(table_name, basestring):
            raise GPUdbException( "Parameter 'table_name' must be a"
                                  "string; given %s"
                                  % str( type( table_name ) ) )
        # Validate input parameter 'record_type'
        if not isinstance( record_type, (GPUdbRecordType) ):
            raise GPUdbException( "Parameter 'record_type' must be of "
                                  "type GPUdbRecordType; given %s"
                                  % str( type( record_type ) ) )
        # Validate input parameter 'batch_size'
        if ( not isinstance(batch_size, int) or (batch_size < 1) ):
            raise GPUdbException( "Parameter 'batch_size' must be greater"
                                  " than zero; given %d" % batch_size )
        # Validate input parameter 'options'
        if not isinstance( options, (dict, type(None)) ):
            raise GPUdbException( "Parameter 'options' must be a"
                                  "dicitonary, if given; given %s"
                                  % str( type( options ) ) )
        # Validate input parameter 'workers'
        if (workers and not isinstance(workers, GPUdbWorkerList)):
            raise GPUdbException( "Parameter 'workers' must be of type "
                                  "GPUdbWorkerList; given %s"
                                  % str( type( workers ) ) )
        # Validate input parameter 'is_table_replicated'
        if not isinstance( is_table_replicated, bool ):
            raise GPUdbException( "Parameter 'is_table_replicated' must be of type "
                                  "a boolean value; given %s"
                                  % str( type( is_table_replicated ) ) )

        # Class level logger so that setting it for ond instance doesn't
        # set it for ALL instances after that change (even if it is
        # outside of the scope of the first instance whose log level was
        # changed
        self.log = logging.getLogger( "gpudb.GPUdbIngestor_instance_"
                                      + str( uuid.uuid4() ) )

        # Handlers need to be instantiated only ONCE for a given module
        # (i.e. not per class instance)
        handler   = logging.StreamHandler()
        formatter = logging.Formatter( "%(asctime)s %(levelname)-8s %(message)s",
                                        "%Y-%m-%d %H:%M:%S.%u%u%u" )
        handler.setFormatter( formatter )
        self.log.addHandler( handler )

        # Prevent logging statements from being duplicated
        self.log.propagate = False

        # Save the parameter values
        self.gpudb                = gpudb
        self.table_name           = table_name
        self.record_type          = record_type
        self.batch_size           = batch_size
        self.options              = options
        self.is_table_replicated  = is_table_replicated
        self.worker_list          = workers

        # Keep track of the current head node being used
        self.__curr_head_node_url = self.gpudb.get_url( stringified = False )

        self.__retry_count = self.__DEFAULT_INSERTION_RETRY_COUNT

        self.count_inserted = 0
        self.count_updated  = 0

        # Keep track of how many times the db client has switched HA clusters
        # in order to decide later if it's time to update the worker queues
        self.num_cluster_switches = self.gpudb.get_num_cluster_switches()

        # Create the primary and shard key builders
        self.shard_key_builder   = _RecordKeyBuilder( self.record_type )
        self.primary_key_builder = _RecordKeyBuilder( self.record_type,
                                                 is_primary_key = True )

        # Save the appropriate key builders
        if self.primary_key_builder.has_key():
            # If both pk and shard keys exist; check that they're not the same
            # If so, set them to be the same
            if ( not self.shard_key_builder.has_key()
                 or self.shard_key_builder.has_same_key( self.primary_key_builder ) ):
                self.shard_key_builder = self.primary_key_builder
        else:
            self.primary_key_builder = None

            if not self.shard_key_builder.has_key():
                self.shard_key_builder = None
        # end saving the key builders

        self.has_primary_key = (self.primary_key_builder is not None)

        # Set up the worker queues
        # ------------------------
        # Boolean flag for primary key related info
        self.update_on_existing_pk = False
        if ( self.options
             and ("update_on_existing_pk" in self.options) ):
            self.update_on_existing_pk = (self.options[ "update_on_existing_pk" ] == "true")
        # end if

        self.worker_queues = []

        # If no worker URLs are provided, get them from the server
        if not self.worker_list:
            # If the table is replicated, then we use only the head node
            self.worker_list = GPUdbWorkerList( self.gpudb,
                                                use_head_node_only = self.is_table_replicated )

        # Create worker queues per worker URL
        for worker in self.worker_list.get_worker_urls():
            # Handle removed ranks
            if not worker:
                self.worker_queues.append( None )
                continue

            try:
                wq = _WorkerQueue( worker,
                                   self.batch_size,
                                   has_primary_key = self.has_primary_key,
                                   update_on_existing_pk = self.update_on_existing_pk )
                self.worker_queues.append( wq )
            except Exception as e:
                raise GPUdbException( GPUdbException.stringify_exception( e ) )
        # end loop over workers

        # Get the number of workers
        if not self.worker_list:
            self.num_ranks = 1
        else:
            self.num_ranks = len( self.worker_list.get_worker_urls() )

        # Very important to know if multi-head IO is actually enabled
        # at the server
        self.is_multihead_enabled = self.worker_list.is_multihead_enabled()

        # Flag for whether to use sharding or not
        self.use_head_node = ( (not self.is_multihead_enabled)
                               or self.is_table_replicated )

        # Set the routing table, iff multi-head I/O is turned on
        # AND the table is not replicated
        self.routing_table = None
        self._shard_version = None
        self._shard_update_time = None
        if ( not self.use_head_node
             and (self.primary_key_builder or self.shard_key_builder) ):

            # Since it's the first time, there's no need to "REconstruct"
            # the queues
            self.__update_worker_queues( self.num_cluster_switches,
                                         do_reconstruct_worker_queues = False )
        # end if

    # end GPUdbIngestor __init__


    def __force_failover( self, curr_url, curr_count_cluster_switches ):
        """Force a high-availability cluster (inter-cluster) or ring-resiliency
        (intra-cluster) failover over, as appropriate.  Check the health of the
        cluster (either head node only, or head node and worker ranks, based on
        the retriever configuration), and use it if healthy.  If no healthy cluster
        is found, then throw an error.  Otherwise, stop at the first healthy cluster.


        Parameters:
            curr_url (str or GPUdb.URL)
                The head node URL of the currently active cluster.
            curr_count_cluster_switches (int)
                The number of times the GPUdb client has switched HA clusters so
                far.

        @throws GPUdbException if a successful failover could not be achieved.
        """
        for i in range(0, self.gpudb.ha_ring_size):
            # Try to switch to a new cluster
            try:
                self.__log_debug( "Forced HA failover attempt #{}".format( i ) )
                self.gpudb._GPUdb__switch_url( curr_url,
                                               curr_count_cluster_switches )
            except GPUdbHAUnavailableException as ex:
                # Have tried all clusters; back to square 1
                raise ex
            except GPUdbFailoverDisabledException as ex:
                # Failover is disabled
                raise ex
            # end try

            # Update the reference points
            curr_url                    = self.gpudb.get_url( stringified = False )
            curr_count_cluster_switches = self.gpudb.get_num_cluster_switches()

            # We did switch to a different cluster; now check the health
            # of the cluster, starting with the head node
            if not self.gpudb.is_kinetica_running( curr_url ):
                continue # try the next cluster because this head node is down
            # end if

            is_cluster_healthy = True
            if self.is_multihead_enabled:
                # Obtain the worker rank addresses
                try:
                    worker_ranks = GPUdbWorkerList( self.gpudb,
                                                    ip_regex = self.worker_list.get_ip_regex(),
                                                    use_head_node_only = self.use_head_node )
                    self.__log_debug( "Got new worker_ranks: {}"
                                      "".format( worker_ranks ) )
                except GPUdbException as ex:
                    # Some problem occurred; move to the next cluster
                    self.__log_debug( "Problem creating worker ranks ({}); "
                                      "moving to next cluster".format( str(ex) ) )
                    continue
                # end try

                # Check the health of all the worker ranks
                for worker_rank in worker_ranks.worker_urls:
                    worker_rank = GPUdb.URL( worker_rank )
                    if ( not self.gpudb.is_kinetica_running( worker_rank ) ):
                        is_cluster_healthy = False
                    # end if
                # end for
            # end if

            if is_cluster_healthy:
                # Save the healthy cluster's URL as the current head node URL
                self.__curr_head_node_url = curr_url
                self.num_cluster_switches = curr_count_cluster_switches
                return
            # end if
        # end for loop

        # If we get here, it means we've failed over across the whole HA ring at least
        # once (could be more times if other threads are causing failover, too)
        error_msg = ("HA failover could not find any healthy cluster (all GPUdb "
                     "clusters with head nodes {} tried)"
                     "".format( [ str(u) for u in self.gpudb.get_head_node_urls()] ) )
        self.__log_debug( error_msg )
        raise GPUdbException( error_msg )
    # end __force_failover


    def __update_worker_queues( self, count_cluster_switches,
                                do_reconstruct_worker_queues = True ):
        """Update the shard mapping for the ingestor.

        Parameters:
            count_cluster_switches (int)
                Integer keeping track of how many times inter-cluster failover
                has happened.
            do_reconstruct_worker_queues (bool)
                When True, the worker queues will be re-constructed based on
                the new cluster configuration.  The records that are already in
                the existing queues will be re-processed to be saved in the
                new queues.

        Returns:
            A boolean flag indicating if the shard mapping was updated.
        """
        # Decide if the worker queues will need to be reconstructed (they will
        # only if multi-head is enabled, it is not a replicated table, and if
        # the user wants to)
        reconstruct_worker_queues = ( do_reconstruct_worker_queues
                                      and (not self.use_head_node) )
        try:
            # Get the sharding assignment ranks
            shard_info = self.gpudb.admin_show_shards()

            if not shard_info.is_ok():
                raise GPUdbException( shard_info.get_error_msg() )

            # Get the shard version
            new_shard_version = shard_info[ C._shard_version ]

            # No-op if the shard version hasn't changed (and it's not the first time)
            if self._shard_version and (self._shard_version == new_shard_version):
                # Also check if the db client has failed over to a different HA
                # ring node
                num_db_ha_switches = self.gpudb.get_num_cluster_switches()
                if (count_cluster_switches == num_db_ha_switches):
                    self.__log_debug( "# cluster switches and shard versions "
                                      "the same" )

                    # Still using the same cluster; but may have done an N+1
                    # failover
                    if reconstruct_worker_queues:
                        # The caller needs to know if we ended up updating the
                        # queues
                        return self.__reconstruct_worker_queues_and_requeue_records()
                    # end if

                    # Not appropriate to update worker queues; then no change
                    # has happened
                    self.__log_debug( "Returning false" )
                    return False # nothing to do
                # end if

                # Update the HA ring node switch tracker
                self.num_cluster_switches = num_db_ha_switches

            # Save the new shard version and also when we're updating the mapping
            self._shard_version = new_shard_version
            self._shard_update_time = time.time()

            # Subtract 1 from each value of the routing_table
            # (because the 1st worker rank is the 0th element in the worker list)
            # TODO: Check if this needs to be aligned with the Java API
            self.routing_table = [(rank-1) for rank in shard_info[ C._shard_ranks ] ]
        except GPUdbException as ex:
            # Couldn't get the current shard assignment info; see if this is due
            # to cluster failure
            if ex.is_connection_failure():
                # Could not update the worker queues because we can't connect
                # to the database
                self.__log_debug( "Had connection failure: {}".format( str(ex) ) )
                # TODO: The Java API doesn't have this bit; need to ensure that
                # the Python API doesn't need it still
                # # Check if the db client has failed over to a different HA
                # # ring node
                # if (self.num_cluster_switches == self.gpudb.get_num_cluster_switches()):
                #     return False # nothing to do; some other problem
                # # Update the HA ring node switch tracker
                # self.num_cluster_switches = self.gpudb.get_num_cluster_switches()

                return False
            else: # unknown error no handled here
                raise ex
            # end if
        # end except

        # If we get here, then we may have done a cluster failover during
        # /admin/show/shards; so update the current head node url & count of
        # cluster switches
        self.__curr_head_node_url = self.gpudb.get_url( stringified = False )
        self.num_cluster_switches      = self.gpudb.get_num_cluster_switches()

        # The worker queues need to be re-constructed when asked for
        # iff multi-head i/o is enabled and the table is not replicated
        if reconstruct_worker_queues:
            self.__reconstruct_worker_queues_and_requeue_records()

        self.__log_debug( "Returning true" )
        return True # the shard mapping was updated indeed
    # end __update_worker_queues



    def __reconstruct_worker_queues_and_requeue_records( self ):
        """Based on a freshly fetched worker list, re-constructs the
        worker queues and re-queues already queued records.

        Returns:
           Boolean indicating whether we ended up reconstructing the worker
           queues or not.
        """
        # Get the latest worker list (use whatever IP regex was used initially)
        new_worker_list = GPUdbWorkerList( self.gpudb,
                                           ip_regex = self.worker_list.get_ip_regex(),
                                           use_head_node_only = self.use_head_node )
        self.__log_debug( "Current worker list: {}".format( str(self.worker_list) ) )
        self.__log_debug( "New worker list:     {}".format( str(new_worker_list) ) )
        if (new_worker_list == self.worker_list):
            return False # nothing to do since the worker list did not change

        # Update the worker list
        self.worker_list  = new_worker_list
        new_workers       = self.worker_list.get_worker_urls()
        new_worker_queues = []
        self.__log_debug( "New workers: {}".format( str(new_workers) ) )

        # Create worker queues per worker URL
        for worker in new_workers:
            # Handle removed ranks
            if not worker:
                new_worker_queues.append( None )
                continue

            try: # adding a queue for a currently active rank
                wq = _WorkerQueue( worker,
                                   self.batch_size,
                                   has_primary_key = self.has_primary_key,
                                   update_on_existing_pk = self.update_on_existing_pk )
                new_worker_queues.append( wq )
            except Exception as e:
                # In case the exception has no message, we need to stringify
                # the exceptio properly to at least get the exception type
                raise GPUdbException( GPUdbException.stringify_exception( e ) )
        # end loop over workers

        # Get the number of workers
        self.num_ranks = len( new_workers )

        # Save the new queue for future use
        old_worker_queues  = self.worker_queues
        self.worker_queues = new_worker_queues

        # Re-queue any existing queued records
        for old_queue in old_worker_queues:
            if old_queue:
                self.insert_records( old_queue.flush() )
        # end loop

        self.__log_debug( "Worker list was updated, returning true" )
        return True # we did change the queues!
    # end __reconstruct_worker_queues_and_requeue_records


    def __is_log_level_trace_enabled( self ):
        """Returns whether the trace log level is enabled.  This is
        often required when we need to log messages very judiciously.
        Since string concatenation takes a long time, we don't want to
        create the log message if trace level is not enabled.
        """
        return self.log.isEnabledFor( logging.TRACE )
    # end __is_log_level_trace_enabled


    def __log_debug( self, message ):
        # Get calling method's information from the stack
        stack = inspect.stack()
        # stack[1] gives the previous/calling function
        filename = stack[1][1].split("/")[-1]
        ln       = stack[1][2]
        func     = stack[1][3]

        self.log.debug( "[GPUdbIngestor::{fn}::{line}::{func}]  {msg}"
                        "".format( fn = filename,
                                   func = func, line = ln,
                                   msg = message ) )
    # end __log_debug

    def __log_trace( self, message ):
        # Get calling method's information from the stack
        stack = inspect.stack()
        # stack[1] gives the previous/calling function
        filename = stack[1][1].split("/")[-1]
        ln       = stack[1][2]
        func     = stack[1][3]

        self.log.trace( "[GPUdbIngestor::{fn}::{line}::{func}]  {msg}"
                        "".format( fn = filename,
                                   func = func, line = ln,
                                   msg = message ) )
    # end __log_trace

    def __log_warn( self, message ):
        self.log.warn( "[GPUdbIngestor] {}".format( message ) )
    # end __log_warn

    def __log_info( self, message ):
        self.log.info( "[GPUdbIngestor] {}".format( message ) )
    # end __log_info

    def __log_error( self, message ):
        self.log.error( "[GPUdbIngestor] {}".format( message ) )
    # end __log_error


    def get_gpudb( self ):
        """Return the instance of GPUdb client used by this ingestor."""
        return self.gpudb
    # end get_gpudb


    @property
    def retry_count( self ):
        """Return the number of times ingestion will be attempted upon
        failure."""
        return self.__retry_count
    # end retry_count

    @retry_count.setter
    def retry_count( self, value ):
        """Sets the number of times ingestion will be attempted upon
        failure.  Must be a positive integer."""
        try:
            value = int( value )
        except:
            raise GPUdbException( "Expected a numeric value, got: '{}'"
                                  "".format( value ) )
        # end try

        # Port values must be within (0, 65536)
        if ( value < 0 ):
            raise GPUdbException( "Expected a positive integer; got '{}'"
                                  "".format( value ) )
        # end if

        self.__retry_count = value
    # end retry_count


    def get_table_name( self ):
        """Return the GPUdb table associated with this ingestor."""
        return self.table_name
    # end get_table_name


    def get_batch_size( self ):
        """Return the batch_size used for this ingestor."""
        return self.batch_size
    # end get_batch_size


    def get_options( self ):
        """Return the options used for this ingestor."""
        return self.options
    # end get_options


    def get_count_inserted( self ):
        """Return the number of records inserted thus far."""
        return self.count_inserted
    # end get_count_inserted


    def get_count_updated( self ):
        """Return the number of records updated thus far."""
        return self.count_updated
    # end get_count_updated


    def set_logger_level( self, log_level ):
        """Set the log level for the GPUdb multi-head i/o module.

        Parameters:
            log_level (int, long, or str)
                A valid log level for the logging module
        """
        try:
            self.log.setLevel( log_level )
        except (ValueError, TypeError, Exception) as ex:
            raise GPUdbException("Invalid log level: '{}'"
                                 "".format( GPUdbException.stringify_exception( ex ) ))
    # end set_client_logger_level


    def __encode_data_for_insertion( self, records, record_encoding = "binary" ):
        """Encode the given records for insertion.

        Parameters:
            records (list)
                A list of un-encoded records.

            record_encoding (str)
                The encoding to use for the insertion.  Allowed values are:

                * 'binary'
                * 'json'

                The default values is 'binary'.

        Returns:
            A list of encoded records.
        """
        # Encode the object into binary if not already encoded
        if record_encoding == "binary":
            if isinstance( records[0], GPUdbRecord ):
                encoded_records = [ record.binary_data for record in records ]
            else:
                encoded_records = [ GPUdbRecord( self.record_type, record ).binary_data for record in records ]
        else:
            if isinstance( record, GPUdbRecord ):
                encoded_records = [ record.column_values for record in records ]
            else:
                encoded_records = records
        # end if-else

        return encoded_records
    # end __encode_data_for_insertion


    def insert_record( self, record, record_encoding = "binary",
                       is_data_encoded = True ):
        """Queues a record for insertion into GPUdb. If the queue reaches the
        {@link #get_batch_size batch size}, all records in the queue will be
        inserted into GPUdb before the method returns. If an error occurs while
        inserting the records, the records will no longer be in the queue nor in
        GPUdb; catch {@link InsertionException} to get the list of records that were
        being inserted if needed (for example, to retry).

        Parameters:
            record (dict, GPUdbRecord, collections.OrderedDict, Record)
                The record to insert.

            record_encoding (str)
                The encoding to use for the insertion.  Allowed values are:

                * 'binary'
                * 'json'

                The default values is 'binary'.


            is_data_encoded (bool)
                Indicates if the data has already been encoded (so that we don't
                do double encoding).  Use ONLY if the data has already been
                encoded.  Default is False.

        @throws InsertionException if an error occurs while inserting.
        """
        # If a dict is given, convert it into a GPUdbRecord object
        if isinstance( record, dict ):
            record = GPUdbRecord( self.record_type, record )

        if not isinstance( is_data_encoded, bool ):
            raise GPUdbException( "Input parameter 'is_data_encoded' must be "
                                  "boolean; given '{}'"
                                  "".format( str(type( is_data_encoded )) ) )

        if not isinstance(record, (list, GPUdbRecord, collections.OrderedDict)):
            raise GPUdbException( "Input parameter 'record' must be a GPUdbRecord or an "
                                  "OrderedDict; given %s" % str(type(record)) )

        if record_encoding.lower() not in ("json", "binary"):
            raise GPUdbException( "Input parameter 'record_encoding' must be "
                                  "one of ['json', 'binary']; given '%s'" % record_encoding )

        # Build the primary and/or shard key(s) for this record
        primary_key = None
        shard_key   = None

        # Build the primary key
        if self.primary_key_builder:
            primary_key = self.primary_key_builder.build( record )

        # Build the shard key
        if self.shard_key_builder:
            shard_key = self.shard_key_builder.build( record )

        # Create a worker queue
        worker_queue = None

        # Get the index of the worker to be used
        if self.use_head_node:
            worker_index = 0
        elif (not shard_key):
            worker_index = random.randint( 0, (self.num_ranks - 1) )
        else:
            # Use the routing table and the shard key to find the right worker
            worker_index = shard_key.route( self.routing_table )
        # end if-else

        # Log which rank this record is going to at the trace level.  Note that
        # since string interpolation takes a demonstrably large time (proved via
        # benchmarking), we need to first check if the log level is on.  That
        # way, we only create the interpolated string when it will be used.
        if self.__is_log_level_trace_enabled():
            self.__log_trace( "Record {} going to worker rank with index {}"
                              "".format( str(record), worker_index ) )
        # end if

        # Check that the index is withing bounds
        if (worker_index >= len(self.worker_queues)):
            raise GPUdbException( "Sharded worker index is out of bound: {} "
                                  "(# worker ranks {})"
                                  "".format( worker_index, len(self.worker_queues) ) )

        # Get the worker
        worker_queue = self.worker_queues[ worker_index ]

        # Insert the record for the worker queue
        queue = worker_queue.insert( record, primary_key )

        # Flush, if necessary (when the worker queue returns a non-empty queue)
        if queue:
            self.__flush( queue, worker_queue.get_url(),
                          is_data_encoded = is_data_encoded )
    # end insert_record


    def insert_records( self, records, record_encoding = "binary",
                        is_data_encoded = True ):
        """Queues a list of records for insertion into GPUdb. If any queue reaches
        the {@link #get_batch_size batch size}, all records in that queue will be
        inserted into GPUdb before the method returns. If an error occurs while
        inserting the queued records, the records will no longer be in that queue
        nor in GPUdb; catch {@link InsertionException} to get the list of records
        that were being inserted (including any from the queue in question and
        any remaining in the list not yet queued) if needed (for example, to
        retry). Note that depending on the number of records, multiple calls to
        GPUdb may occur.

        Parameters:
            records (GPUdbRecord, collections.OrderedDict, Record)
                The records to insert.

            record_encoding (str)
                The encoding to use for the insertion.  Allowed values are:

                * 'binary'
                * 'json'

                The default values is 'binary'.

            is_data_encoded (bool)
                Indicates if the data has already been encoded (so that we don't
                do double encoding).  Use ONLY if the data has already been
                encoded.  Default is False.

        @throws InsertionException if an error occurs while inserting
        """
        if not records:
            return # nothing to do!

        # If a single record is provided, promote it to a list
        records = records if isinstance( records, list ) else [ records ]

        if record_encoding not in ("json", "binary"):
            raise GPUdbException( "Input parameter 'record_encoding' must be "
                                  "one of ['json', 'binary']; given '%s'"
                                  % record_encoding )

        if not isinstance( is_data_encoded, bool ):
            raise GPUdbException( "Input parameter 'is_data_encoded' must be "
                                  "boolean; given '{}'"
                                  "".format( str(type( is_data_encoded )) ) )

        for record in records:
            try:
                self.insert_record( record, record_encoding,
                                    is_data_encoded = is_data_encoded )
            except InsertionException as e:
                # Add the remaining records that could not be inserted
                uninserted_records = e.get_records()
                remaining_records = records[ records.index( record ) :  ]
                uninserted_records.extend( remaining_records )

                raise
            # done handling the error case
    # end insert_records




    def flush( self, forced_flush = True, is_data_encoded = True ):
        """Ensures that any queued records are inserted into GPUdb. If an error
        occurs while inserting the records from any queue, the records will no
        longer be in that queue nor in GPUdb; catch {@link InsertionException} to
        get the list of records that were being inserted if needed (for example,
        to retry). Other queues may also still contain unflushed records if
        this occurs.

        Parameters:
            forced_flush (bool)
                Boolean flag indicating whether a user invoked this method or
                an internal method called it.

            is_data_encoded (bool)
                Indicates if the data has already been encoded (so that we don't
                do double encoding).  Use ONLY if the data has already been
                encoded.  Default is False.

        @throws InserttionException if an error occurs while inserting records.
        """
        for worker in self.worker_queues:
            if not worker:
                continue # skipping empty workers

            queue = worker.flush()
            # Actually insert the records
            self.__flush( queue, worker.get_url(), forced_flush = forced_flush,
                          is_data_encoded = is_data_encoded )
    # end flush


    def __insert_records_to_url( self, url = None, data = None,
                                 encoding = None, options = {} ):
        """Makes an /insert/records call to the given URL using the internally
        stored :class:`GPUdb` object.  The returns value is the same as
        :meth:`GPUdb.insert_records`.
        """
        data = data if isinstance( data, list ) else ( [] if (data is None) else [ data ] )
        assert isinstance( encoding, (basestring, type( None ))), "__insert_records_to_url(): Argument 'encoding' must be (one) of type(s) '(basestring, type( None ))'; given %s" % type( encoding ).__name__
        assert isinstance( options, (dict)), "__insert_records_to_url(): Argument 'options' must be (one) of type(s) '(dict)'; given %s" % type( options ).__name__

        obj = {}
        obj['table_name'] = self.table_name
        obj['list_encoding'] = encoding
        obj['options'] = self.gpudb._GPUdb__sanitize_dicts( options )

        record_type = self.record_type.record_type
        if (encoding == 'binary'):
            # Convert the objects to proper Records
            use_object_array, data = _Util.convert_binary_data_to_cext_records( self.gpudb,
                                                                                self.table_name,
                                                                                data,
                                                                                record_type )

            if use_object_array:
                # First tuple element must be a RecordType or a Schema from the c-extension
                obj['list'] = (data[0].type, data) if data else ()
            else: # use avro-encoded bytes for the data
                obj['list'] = data

            obj['list_str'] = []
        else:
            obj['list_str'] = data
            obj['list'] = () # needs a tuple for the c-extension
            use_object_array = True
        # end if


        if use_object_array:
            response = self.gpudb._GPUdb__submit_request( '/insert/records', obj,
                                                          url = url,
                                                          convert_to_attr_dict = True,
                                                          get_req_cext = True )
        else:
            response = self.gpudb._GPUdb__submit_request( '/insert/records', obj,
                                                          url = url,
                                                          convert_to_attr_dict = True )

        if not response.is_ok():
            return response

        return response
    # end __insert_records_to_url


    def __flush( self, queue, worker_url,
                 forced_flush = False,
                 record_encoding = "binary",
                 is_data_encoded = True ):
        """Internal method to flush--actually insert--the records to GPUdb.

        Parameters:
            queue (list)
                List of records to insert

            worker_url (str)
                The URL to the GPUdb server to which to send the records.

            forced_flush (bool)
                If True, then somebody intends to forcefully flush the given
                records.  Default value is False.

            record_encoding (str)
                The encoding to use for the insertion.  Allowed values are:

                * 'binary'
                * 'json'

                The default values is 'binary'.

            is_data_encoded (bool)
                Indicates if the data has already been encoded (so that we don't
                do double encoding).  Use ONLY if the data has already been
                encoded.  Default is False.
        """
        if not queue:
            return # nothing to do

        if record_encoding.lower() not in ("json", "binary"):
            raise GPUdbException( "Input parameter 'record_encoding' must be "
                                  "one of ['json', 'binary']; given '%s'" % record_encoding )

        retries = self.__retry_count
        try:
            # Encode the data, if necessary
            if not is_data_encoded:
                encoded_data = self.__encode_data_for_insertion( queue,
                                                                 record_encoding = record_encoding )
            else:
                # The data is already encoded
                encoded_data = queue
            # end if

            while True:
                # Save a snapshot of the state of the object pre-insertion attempt
                insertion_attempt_timestamp = time.time()
                curr_url = self.__curr_head_node_url
                current_count_cluster_switches = self.num_cluster_switches

                try:
                    self.__log_debug( "Sending {} records to {}"
                                      "".format( len(queue), worker_url ) )

                    # # Note: The following debug is for developer debugging **ONLY**.
                    # #       NEVER have this checked in uncommented since it will
                    # #       slow down everything by printing the whole queue!
                    # self.__log_debug( "Inserting records: {}".format( queue) )

                    url = GPUdb.URL( worker_url )
                    insert_rsp =  self.__insert_records_to_url( url = url,
                                                                data = encoded_data,
                                                                encoding = record_encoding,
                                                                options = self.options )

                    # Throw an error if there was any problem (the exception
                    # blocks will handle retrying)
                    if not insert_rsp.is_ok():
                        raise GPUdbException( insert_rsp.get_error_msg() )
                    # end if

                    # Update the insert and update counts
                    self.count_inserted += insert_rsp[ C._count_inserted ]
                    self.count_updated  += insert_rsp[ C._count_updated  ]

                    # Check if shard re-balancing is under way at the server; if so,
                    # we need to update the shard mapping
                    if ( (C._data_rerouted in insert_rsp.info)
                         and (insert_rsp.info[ C._data_rerouted ] ==  C._true) ) :

                        self.__update_worker_queues( current_count_cluster_switches )
                    # end inner if

                    break # out of the while loop
                except GPUdbUnauthorizedAccessException as ex:
                    # Any permission related problem should get propagated
                    self.__log_debug( "Caught GPUdb UNAUTHORIZED exception: "
                                      "{}".format( str(ex) ) )
                    raise
                except GPUdbException as ex:
                    self.__log_debug( "Caught GPUdb (original) exception: {}"
                                      "".format( str(ex) ) )
                    retry = False

                    # If some connection issue occurred, we want to force an HA failover
                    if ( isinstance(ex, (GPUdbConnectionException, GPUdbExitException))
                         or ex.had_connection_failure() ):
                        self.__log_debug( "Caught EXIT exception or had other "
                                          "connection failure: {}"
                                          "".format( str(ex) ) )
                        # We did encounter an HA failover trigger
                        try:
                            # Switch to a different cluster in the HA ring, if any
                            self.__force_failover( curr_url,
                                                   current_count_cluster_switches )

                            # If we succesfully failed over, then we should
                            # retry the insertion
                            retry = True
                        except GPUdbException as ex2:
                            # We've now tried all the HA clusters and circled back;
                            # propagate the error to the user, but only there
                            # are no more retries left
                            self.__log_debug( "Caught (second) exception: {}"
                                              "".format( str(ex2) ) )
                            raise GPUdbException( "{orig}; {second}"
                                                  "".format( orig = str(ex),
                                                             second = str(ex2) ),
                                                  had_connection_failure = True )
                        # end try
                    else:
                        # For debugging purposes only (can be very useful!)
                        self.__log_debug( "Caught GPUdbException: {}"
                                          "".format( str(ex) ) )
                    # end if

                    # Update the worker queues since we've failed over to a
                    # different cluster
                    updated_worker_queues = self.__update_worker_queues( current_count_cluster_switches )

                    if ( updated_worker_queues
                         or (insertion_attempt_timestamp < self._shard_update_time) ):
                        retry = True
                    # end if

                    if retry:
                        # Now that we've switched to a different cluster, re-insert
                        # since no worker queue has these records any more (but the
                        # records may go to a worker queue different from the one
                        # they came from)
                        retries = (retries - 1)

                        try:
                            self.__log_debug( "Retrying insertion of the queued records" )
                            self.insert_records( queue,
                                                 record_encoding = record_encoding,
                                                 is_data_encoded = is_data_encoded )

                            # If the user intends a forceful flush, i.e. the public flush()
                            # was invoked, then make sure that the records get flushed
                            if forced_flush:
                                self.flush( forced_flush    = forced_flush,
                                            is_data_encoded = is_data_encoded )
                            # end if

                            break; # out of the while loop
                        except Exception as ex2:
                            # Re-setting the exception since we may re-try again
                            if (retries <= 0):
                                raise ex2
                            # end if
                        # end try
                    else:
                        self.__log_debug( "NOT retrying insertion of the queued records" )
                    # end if

                    # If we still have retries left, then we'll go into the next
                    # iteration of the infinite while loop; otherwise, propagate
                    # the exception
                    if (retries > 0):
                        retries = (retries - 1)
                    else:
                        # No more retries; propagate exception to user along with the
                        # failed queue of records
                        raise InsertionException( str(ex), queue )
                    # end if
                except Exception as ex:
                    ex_str = GPUdbException.stringify_exception( ex )
                    self.__log_debug( "Caught regular exception: {}"
                                      "".format( ex_str ) )
                    # Insertion failed, but maybe due to shard mapping changes (due to
                    # cluster reconfiguration)? Check if the mapping needs to be updated
                    # or has been updated by another thread already after the
                    # insertion was attemtped
                    updated_worker_queues = self.__update_worker_queues( current_count_cluster_switches )

                    retry = False
                    retry = ( updated_worker_queues
                              or (insertion_attempt_timestamp < self._shard_update_time) )

                    if retry:
                        # We need to try inserting the records again since no worker
                        # queue has these records any more (but the records may
                        # go to a worker queue different from the one they came from)
                        retries = (retries - 1)

                        try:
                            self.__log_debug( "Retrying insertion of the queued records" )
                            self.insert_records( queue,
                                                 record_encoding = record_encoding,
                                                 is_data_encoded = is_data_encoded )

                            # If the user intends a forceful flush, i.e. the public flush()
                            # was invoked, then make sure that the records get flushed
                            if forced_flush:
                                self.flush( forced_flush    = forced_flush,
                                            is_data_encoded = is_data_encoded )
                            # end if

                            break # out of the while loop
                        except Exception as ex2:
                            # Re-setting the exception since we may re-try again
                            ex = ex2
                        # end try
                    else:
                        self.__log_debug( "NOT retrying insertion of the queued records" )
                    # end if

                    # If we still have retries left, then we'll go into the next
                    # iteration of the infinite while loop; otherwise, propagate
                    # the exception
                    if (retries > 0):
                        retries = (retries - 1)
                    else:
                        # No more retries; propagate exception to user along with the
                        # failed queue of records
                        raise ex
                    # end if
                # end inner try
            # end while
        except Exception as ex:
            traceback_msg = "".join( traceback.format_exception( sys.exc_info()[0],
                                                                 sys.exc_info()[1],
                                                                 sys.exc_info()[2] ) )
            self.__log_debug( "Got stacktrace: {}".format( traceback_msg ) )
            raise InsertionException( GPUdbException.stringify_exception( ex ),
                                      queue )
        # end outer try
    # end __flush


# end class GPUdbIngestor




class RecordRetriever:
    """Retrieves records from all worker ranks directly.  If multi-head
    retrieval is not set up, then automatically retrieves records from the
    head node.
    """

    def __init__( self,
                  gpudb,
                  table_name,
                  record_type,
                  workers = None,
                  is_table_replicated = False ):
        """Initializes the RecordRetriever instance.

        Parameters:
            gpudb (GPUdb)
                The client handle through which the retrieval process
                is to be conducted.
            table_name (str)
                The name of the table from which records will be fetched.
                Must be an existing table.
            record_type (GPUdbRecordType)
                The type for the records which will be retrieved; must match
                the type of the given table.
            workers (GPUdbWorkerList)
                Optional parameter.  A list of GPUdb worker rank addresses.
            is_table_replicated (bool)
                Optional boolean flag indicating whether the table is replicated; if
                True, then multi-head ingestion will not be used (but the head node
                would be used for ingestion instead).  This is due to GPUdb not
                supporting multi-head retrieval on replicated tables which are
                un-sharded by design.
        """

        # Validate input parameter 'gpudb'
        if not isinstance(gpudb, GPUdb):
            raise GPUdbException( "Parameter 'gpudb' must be of "
                                  "type GPUdb; given %s"
                                  % str( type( gpudb ) ) )
        # Validate input parameter 'table_name'
        if not isinstance(table_name, basestring):
            raise GPUdbException( "Parameter 'table_name' must be a"
                                  "string; given %s"
                                  % str( type( table_name ) ) )
        # Validate input parameter 'record_type'
        if not isinstance( record_type, GPUdbRecordType ):
            raise GPUdbException( "Parameter 'record_type' must be of "
                                  "type GPUdbRecordType; given %s"
                                  % str( type( record_type ) ) )
        # Validate input parameter 'workers'
        if (workers and not isinstance(workers, GPUdbWorkerList)):
            raise GPUdbException( "Parameter 'workers' must be of type "
                                  "GPUdbWorkerList; given %s"
                                  % str( type( workers ) ) )
        # Validate input parameter 'is_table_replicated'
        if not isinstance( is_table_replicated, bool ):
            raise GPUdbException( "Parameter 'is_table_replicated' must be of type "
                                  "a boolean value; given %s"
                                  % str( type( is_table_replicated ) ) )

        # Class level logger so that setting it for ond instance doesn't
        # set it for ALL instances after that change (even if it is
        # outside of the scope of the first instance whose log level was
        # changed
        self.log = logging.getLogger( "gpudb.RecordRetriever_instance_"
                                      + str( uuid.uuid4() ) )

        # Handlers need to be instantiated only ONCE for a given module
        # (i.e. not per class instance)
        handler   = logging.StreamHandler()
        formatter = logging.Formatter( "%(asctime)s %(levelname)-8s %(message)s",
                                        "%Y-%m-%d %H:%M:%S.%u%u%u" )
        handler.setFormatter( formatter )
        self.log.addHandler( handler )

        # Prevent logging statements from being duplicated
        self.log.propagate = False

        # Save the parameter values
        self.gpudb       = gpudb
        self.table_name  = table_name
        self.record_type = record_type
        self.worker_list = workers
        self.is_table_replicated = is_table_replicated

        # Keep track of the current head node being used
        self.__curr_head_node_url = self.gpudb.get_url( stringified = False )

        # Keep track of how many times the db client has switched HA clusters
        # in order to decide later if it's time to update the worker queues
        self.num_cluster_switches = self.gpudb.get_num_cluster_switches()

        # Create the shard key builder
        self.shard_key_builder = _RecordKeyBuilder( self.record_type )

        # If no shard columns, then check if there are primary keys
        if not self.shard_key_builder.has_key():
            self.shard_key_builder = _RecordKeyBuilder( self.record_type,
                                                        is_primary_key = True )
        if not self.shard_key_builder.has_key():
            self.shard_key_builder = None


        # Set up the worker queues
        # ------------------------

        # If no worker URLs are provided, get them from the server
        if not self.worker_list:
            self.worker_list = GPUdbWorkerList( self.gpudb,
                                                use_head_node_only = self.is_table_replicated )

        # Create worker queues per worker URL
        self.worker_queues = []
        for worker in self.worker_list.get_worker_urls():
            # Handle removed ranks
            if not worker:
                self.worker_queues.append( None )
                continue

            try:
                wq = _WorkerQueue( worker,
                                   capacity = 1 ) # using one for now..........
                self.worker_queues.append( wq )
            except Exception as ex:
                raise GPUdbException( GPUdbException.stringify_exception( ex ) )
        # end loop over workers

        # Get the number of workers
        if not self.worker_list:
            self.num_ranks = 1
        else:
            self.num_ranks = len( self.worker_list.get_worker_urls() )

        # Very important to know if multi-head IO is actually enabled
        # at the server
        self.is_multihead_enabled = self.worker_list.is_multihead_enabled()

        # Flag for whether to use sharding or not
        self.use_head_node = ( (not self.is_multihead_enabled)
                               or self.is_table_replicated )

        self.routing_table = None
        self._shard_version = None
        self._shard_update_time = None
        if ( self.is_multihead_enabled
             and self.shard_key_builder ):

            # Since it's the first time, there's no need to "REconstruct"
            # the queues
            self.__update_worker_queues( self.num_cluster_switches,
                                         do_reconstruct_worker_queues = False )
        # end if
    # end RecordRetriever __init__


    def __is_log_level_trace_enabled( self ):
        """Returns whether the trace log level is enabled.  This is
        often required when we need to log messages very judiciously.
        Since string concatenation takes a long time, we don't want to
        create the log message if trace level is not enabled.
        """
        return self.log.isEnabledFor( logging.TRACE )
    # end __is_log_level_trace_enabled


    def __log_debug( self, message ):
        # Get calling method's information from the stack
        stack = inspect.stack()
        # stack[1] gives the previous/calling function
        filename = stack[1][1].split("/")[-1]
        ln       = stack[1][2]
        func     = stack[1][3]

        self.log.debug( "[RecordRetriever]::{fn}::{line}::{func}]  {msg}"
                        "".format( fn = filename,
                                   func = func, line = ln,
                                   msg = message ) )
    # end __debug

    def __log_trace( self, message ):
        # Get calling method's information from the stack
        stack = inspect.stack()
        # stack[1] gives the previous/calling function
        filename = stack[1][1].split("/")[-1]
        ln       = stack[1][2]
        func     = stack[1][3]

        self.log.trace( "[RecordRetriever]::{fn}::{line}::{func}]  {msg}"
                        "".format( fn = filename,
                                   func = func, line = ln,
                                   msg = message ) )
    # end __log_trace

    def __log_warn( self, message ):
        self.log.warn( "[RecordRetriever] {}".format( message ) )
    # end __log_warn

    def __log_info( self, message ):
        self.log.info( "[RecordRetriever] {}".format( message ) )
    # end __log_info

    def __log_error( self, message ):
        self.log.error( "[RecordRetriever] {}".format( message ) )
    # end __log_error


    def __force_failover( self, old_url, curr_count_cluster_switches ):
        """Force a high-availability cluster (inter-cluster) or ring-resiliency
        (intra-cluster) failover over, as appropriate.  Check the health of the
        cluster (either head node only, or head node and worker ranks, based on
        the retriever configuration), and use it if healthy.  If no healthy cluster
        is found, then throw an error.  Otherwise, stop at the first healthy cluster.

        Parameters:
            old_url (str or GPUdb.URL)
                The URL being used before forcing failover.
            curr_count_cluster_switches (int)
                The number of times the GPUdb client has switched HA clusters so
                far.

        @throws GPUdbException if a successful failover could not be achieved.
        """
        self.__log_debug( "Forced failover begin..." )

        # We'll need to know which URL we're using at the moment
        curr_url = old_url

        for i in range(0, self.gpudb.ha_ring_size):
            # Try to switch to a new cluster
            try:
                self.__log_debug( "Forced HA failover attempt #{}".format( i ) )
                self.gpudb._GPUdb__switch_url( curr_url,
                                               curr_count_cluster_switches )
            except GPUdbUnauthorizedAccessException as ex:
                # Any permission related problem should get propagated
                raise
            except GPUdbHAUnavailableException as ex:
                # Have tried all clusters; back to square 1
                raise ex
            except GPUdbFailoverDisabledException as ex:
                # Failover is disabled
                raise ex
            # end try

            # Update the reference points
            curr_url                    = self.gpudb.get_url( stringified = False )
            curr_count_cluster_switches = self.gpudb.get_num_cluster_switches()

            # We did switch to a different cluster; now check the health
            # of the cluster, starting with the head node
            if not self.gpudb.is_kinetica_running( curr_url ):
                continue # try the next cluster because this head node is down
            # end if

            # Check if we switched the rank-0 URL
            did_switch_url = (curr_url != old_url)

            is_cluster_healthy = True
            if self.is_multihead_enabled:
                # Obtain the worker rank addresses
                try:
                    worker_ranks = GPUdbWorkerList( self.gpudb,
                                                    ip_regex = self.worker_list.get_ip_regex(),
                                                    use_head_node_only = self.use_head_node )
                except GPUdbException as ex:
                    # Some problem occurred; move to the next cluster
                    continue
                # end try

                # Check the health of all the worker ranks
                for worker_rank in worker_ranks.worker_urls:
                    worker_rank = GPUdb.URL( worker_rank )
                    if ( not self.gpudb.is_kinetica_running( worker_rank ) ):
                        is_cluster_healthy = False
                    # end if
                # end for
            # end if

            if is_cluster_healthy:
                # Save the healthy cluster's URL as the current head node URL
                self.__curr_head_node_url = curr_url
                self.num_cluster_switches = curr_count_cluster_switches
                self.__log_debug( "Did we actually switch the URL? {}"
                                  "".format( did_switch_url ) )
                return did_switch_url
            # else, this cluster is not healthy; try switching again
        # end for loop

        # If we get here, it means we've failed over across the whole HA ring at least
        # once (could be more times if other threads are causing failover, too)
        error_msg = ("HA failover could not find any healthy cluster (all GPUdb "
                     "clusters with head nodes {} tried)"
                     "".format( [ str(u) for u in self.gpudb.get_head_node_urls() ] ) )
        self.__log_debug( error_msg )
        raise GPUdbException( error_msg )
    # end __force_failover



    def __update_worker_queues( self, count_cluster_switches,
                                do_reconstruct_worker_queues = True ):
        """Updates the shard mapping based on the latest cluster configuration.
        Optionally, also reconstructs the worker queues based on the new
        sharding.

        Parameters:
            count_cluster_switches (int)
                Integer keeping track of how many times inter-cluster failover
                has happened.
            do_reconstruct_worker_queues (bool)
                When True, the worker queues will be re-constructed based on
                the new cluster configuration.  The records that are already in
                the existing queues will be re-processed to be saved in the
                new queues.

        Returns:
            A boolean flag indicating if the shard mapping was updated.
        """
        # Decide if the worker queues will need to be reconstructed (they will
        # only if multi-head is enabled, it is not a replicated table, and if
        # the user wants to)
        reconstruct_worker_queues = ( do_reconstruct_worker_queues
                                      and (not self.use_head_node) )
        self.__log_debug( "Reconstruct worker URLs?: {}"
                          "".format( reconstruct_worker_queues ) )

        try:
            # Get the sharding assignment ranks
            shard_info = self.gpudb.admin_show_shards()

            if not shard_info.is_ok():
                raise GPUdbException( shard_info.get_error_msg() )

            # Get the shard version
            new_shard_version = shard_info[ C._shard_version ]

            # No-op if the shard version hasn't changed (and it's not the first time)
            if self._shard_version and (self._shard_version == new_shard_version):
                # Also check if the db client has failed over to a different HA
                # ring node
                num_cluster_switches = self.gpudb.get_num_cluster_switches()
                if (count_cluster_switches == num_cluster_switches):
                    self.__log_debug( "# cluster switches and shard versions "
                                      "the same" )

                    # Still using the same cluster; but may have done an N+1
                    # failover
                    if reconstruct_worker_queues:
                        # The caller needs to know if we ended up updating the
                        # queues
                        return self.__reconstruct_worker_queues()
                    # end if

                    # Not appropriate to update worker queues; then no change
                    # has happened
                    self.__log_debug( "Returning false" )
                    return False # nothing to do
                # end if

                # Update the HA ring node switch tracker
                self.num_cluster_switches = num_cluster_switches
            # end if

            # Save the new shard version and also when we're updating the mapping
            self._shard_version = new_shard_version
            self._shard_update_time = time.time()

            # Subtract 1 from each value of the routing_table
            # (because the 1st worker rank is the 0th element in the worker list)
            # TODO: Check if this needs to be aligned with the Java API
            self.routing_table = [(rank-1) for rank in shard_info[ C._shard_ranks ] ]
        except GPUdbException as ex:
            # Couldn't get the current shard assignment info; see if this is due
            # to cluster failure
            if ex.is_connection_failure():
                # Could not update the worker queues because we can't connect
                # to the database
                self.__log_debug( "Had connection failure: {}".format( str(ex) ) )

                return False
            else: # unknown error no handled here
                raise ex
            # end if
        # end except

        # If we get here, then we may have done a cluster failover during
        # /admin/show/shards; so update the current head node url & count of
        # cluster switches
        self.__curr_head_node_url = self.gpudb.get_url( stringified = False )
        self.num_cluster_switches = self.gpudb.get_num_cluster_switches()

        # The worker queues need to be re-constructed when asked for
        # iff multi-head i/o is enabled and the table is not replicated
        if reconstruct_worker_queues:
            self.__reconstruct_worker_queues()

        self.__log_debug( "Returning true" )
        return True # the shard mapping was updated indeed
    # end __update_worker_queues



    def __reconstruct_worker_queues( self ):
        """Based on a freshly fetched worker list, re-constructs the
        worker URLs.
        """
        # Re-construct the existing worker queues and re-shard the currently
        # queued records
        new_worker_queues = []

        # Get the latest worker list (use whatever IP regex was used initially)
        new_worker_list = GPUdbWorkerList( self.gpudb,
                                           self.worker_list.get_ip_regex(),
                                           use_head_node_only = self.use_head_node )
        self.__log_debug( "Current worker list: {}".format( self.worker_list ) )
        self.__log_debug( "New worker list:     {}".format( new_worker_list ) )
        if (new_worker_list == self.worker_list):
            self.__log_debug( "Worker list remained the same; returning false" );
            return False # the worker list did not change

        # Update the worker list
        self.worker_list = new_worker_list
        new_workers      = self.worker_list.get_worker_urls()

        # Create worker queues per worker URL
        for worker in new_workers:
            # Handle removed ranks
            if not worker:
                new_worker_queues.append( None )
                continue

            try: # adding a queue for a currently active rank
                wq = _WorkerQueue( worker,
                                   capacity = 1 ) # using one for now..........
                new_worker_queues.append( wq )
            except Exception as ex:
                raise GPUdbException( GPUdbException.stringify_exception( ex ) )
        # end loop over workers

        # Get the number of workers
        self.num_ranks = len( new_workers )

        # Save the new queue for future use
        self.worker_queues = new_worker_queues

        self.__log_debug( "Worker list was updated, returning true" )
        return True # we did change the URLs!
    # end __reconstruct_worker_queues


    def set_logger_level( self, log_level ):
        """Set the log level for the GPUdb multi-head i/o module.

        Parameters:
            log_level (int, long, or str)
                A valid log level for the logging module
        """
        try:
            self.log.setLevel( log_level )
        except (ValueError, TypeError, Exception) as ex:
            ex_str = GPUdbException.stringify_exception( ex )
            raise GPUdbException("Invalid log level: '{}'".format( ex_str ))
    # end set_client_logger_level



    def __get_records_from_url( self, url = None, options = {} ):
        """Makes a /get/records call to the given URL using the internally
        stored :class:`GPUdb` object.  The returns value is the same as
        :meth:`GPUdb.get_records`.
        """
        assert isinstance( options, (dict)), "__get_records_from_url(): Argument 'options' must be (one) of type(s) '(dict)'; given %s" % type( options ).__name__

        # Create the payload
        obj = {}
        obj[ 'table_name'] = self.table_name
        obj[ 'offset'    ] = 0
        obj[ 'limit'     ] = self.gpudb.END_OF_SET
        obj[ 'encoding'  ] = self.gpudb.encoding
        obj[ 'options'   ] = self.gpudb._GPUdb__sanitize_dicts( options )

        response = self.gpudb._GPUdb__submit_request( '/get/records', obj,
                                                      url = url,
                                                      convert_to_attr_dict = True )
        return response
    # end __get_records_from_url


    def get_records_by_key( self, key_values, expression = "", options = None ):
        """Fetches the record(s) from the appropriate worker rank directly
        (or, if multi-head record retrieval is not set up, then from the
        head node) that map to the given shard key.

        Parameters:

            key_values (list or dict)
                Values for the sharding columns of the record to fetch either in
                a list (then it is assumed to be in the order of the sharding
                keys in the record type) or a dict.  Must not have any missing
                sharding/primary column value or any extra column values.

            expression (str)
                Optional parameter.  If given, it is passed to /get/records as
                a filter expression.

            options (dict of str to str or None)
                Any /get/records options to be passed onto the GPUdb server.  Optional
                parameter.

        Returns:
            The decoded records.
        """
        # Validate input parameter 'options'
        if not isinstance( options, (dict, type(None)) ):
            raise GPUdbException( "Parameter 'options' must be a"
                                  "dicitonary, if given; given %s"
                                  % str( type( options ) ) )


        # If there is no shard key AND the column names aren't given, we can't do this
        if ( (not self.shard_key_builder)
             and (not isinstance( key_values, dict )) ):
            raise GPUdbException( "Cannot get key from unsharded table '%s'"
                                  % self.table_name )

        # Create the expression based on the record's sharded columns' values
        # and any enveloping expression given by the user
        orig_expression = expression
        if isinstance( key_values, dict ):
            # We can build an expression if the column names are given
            # regardless of sharding on the table
            expression = self.shard_key_builder.build_expression_with_dict( key_values,
                                                                            expression )
        elif not expression:
            expression = self.shard_key_builder.build_expression_with_key_values_only( key_values )
        else:
            expression = ( "("
                           + self.shard_key_builder.build_expression_with_key_values_only( key_values )
                          + ") and (" + expression + ")" )
        # end if

        # Set up the options
        if (options is None):
            options = {}
        options["expression"] = expression
        options["fast_index_lookup"] = "true"

        # We may need the timestamp later
        retrieval_attempt_timestamp = time.time()

        curr_url = self.__curr_head_node_url
        curr_count_cluster_switches = self.num_cluster_switches

        try:
            # Get the appropriate worker
            if self.use_head_node: # multi-head is turned off or it's a replicated table
                worker_index = 0
            else: # use sharding to find the appropriate worker
                # Build the shard key
                shard_key = self.shard_key_builder.build_key_with_shard_values_only( key_values )
                # Get the sharded worker
                worker_index = shard_key.route( self.routing_table )
            # end if

            # Check that the index is withing bounds
            if (worker_index >= len(self.worker_queues)):
                raise GPUdbException( "Sharded worker index is out of bound: {} "
                                      "(# worker ranks {})"
                                      "".format( worker_index, len(self.worker_queues) ) )
            # Get the worker
            worker_queue = self.worker_queues[ worker_index ]

            # Find which worker to send the query to
            url = GPUdb.URL( worker_queue.get_url() )

            # Log which rank this record is going to at the trace level.  Note that
            # since string interpolation takes a demonstrably large time (proved via
            # benchmarking), we need to first check if the log level is on.  That
            # way, we only create the interpolated string when it will be used.
            if self.__is_log_level_trace_enabled():
                self.__log_trace( "Retrieving key values {} from worker at {}"
                                  "".format( key_values, url ) )
            # end if

            # Send the /get/records query to the appropriate worker
            gr_rsp = self.__get_records_from_url( url = url,
                                                  options = options )

            if not gr_rsp.is_ok():
                raise GPUdbException( gr_rsp.get_error_msg() )
            # Decode the records (using the C-extension RecordType object)
            records = GPUdbRecord.decode_binary_data( self.record_type.record_type,
                                                      gr_rsp["records_binary"] )

            # Replace the encoded records in the response with the decoded records
            gr_rsp["data"]    = records
            gr_rsp["records"] = records

            return gr_rsp
        except GPUdbUnauthorizedAccessException as ex:
            # Any permission related problem should get propagated
            self.__log_debug( "Caught GPUdb UNAUTHORIZED exception: "
                              "{}".format( str(ex) ) )
            raise
        except GPUdbException as ex:
            self.__log_debug( "Caught GPUdb exception: {}"
                              "".format( str(ex) ) )

            did_failover_succeed = False

            # If some connection issue occurred, we want to force an HA failover
            if ( isinstance(ex, (GPUdbConnectionException, GPUdbExitException))
                 or ex.had_connection_failure() ):
                self.__log_debug( "Caught EXIT exception or had other "
                                  "connection failure: {}"
                                  "".format( str(ex) ) )
                # We did encounter an HA failover trigger
                try:
                    # Switch to a different cluster in the HA ring, if any
                    self.__force_failover( curr_url, curr_count_cluster_switches )
                    did_failover_succeed = True
                except GPUdbException as ex2:
                    # We've now tried all the HA clusters and circled back;
                    # propagate the error to the user, but only there
                    # are no more retries left
                    raise GPUdbException( "{orig}; {second}"
                                          "".format( orig = str(ex),
                                                     second = str(ex2) ),
                                          had_connection_failure = True )
                # end try
            else:
                # For debugging purposes only (can be very useful!)
                self.__log_debug( "Caught GPUdbException: {}"
                                  "".format( str(ex) ) )
            # end if

            self.__log_debug( "Did failover succeed? {}"
                              "".format( did_failover_succeed ) )

            # Update the worker queues since we've failed over to a
            # different cluster
            self.__log_debug( "Updating worker queues" )
            updated_worker_queues = self.__update_worker_queues( curr_count_cluster_switches )
            self.__log_debug( "Did we update the worker queue? {}"
                              "".format( updated_worker_queues ) )

            retry = ( did_failover_succeed
                      or updated_worker_queues
                      or (retrieval_attempt_timestamp < self._shard_update_time) )

            if retry:
                # Now that we've switched to a different cluster, re-insert
                # since no worker queue has these records any more (but the
                # records may go to a worker queue different from the one
                # they came from)
                try:
                    self.__log_debug( "Retrying fetching the records" )
                    return self.get_records_by_key( key_values, orig_expression,
                                                    options )
                except Exception as ex2:
                    # Re-setting the exception since we may re-try again
                    raise GPUdbException( GPUdbException.stringify_exception( ex2 ) )
                # end try
            # end if

            raise GPUdbException( str(ex) )
        except Exception as ex:
            ex_str = GPUdbException.stringify_exception( ex )
            self.__log_debug( "Caught regular exception: {}"
                              "".format( ex_str ) )
            # Retrieval failed, but maybe due to shard mapping changes (due to
            # cluster reconfiguration)? Check if the mapping needs to be updated
            # or has been updated by another thread already after the
            # insertion was attemtped
            updated_worker_queues = self.__update_worker_queues( curr_count_cluster_switches )

            retry = False
            retry = ( updated_worker_queues
                      or (retrieval_attempt_timestamp < self._shard_update_time) )

            if retry:
                # We need to try inserting the records again since no worker
                # queue has these records any more (but the records may
                # go to a worker queue different from the one they came from)
                try:
                    self.__log_debug( "Retrying fetching the records" )
                    return self.get_records_by_key( key_values, orig_expression,
                                                    options )
                except Exception as ex2:
                    # Re-setting the exception since we may re-try again
                    raise GPUdbException( GPUdbException.stringify_exception( ex2 ) )
                # end try
            # end if

            raise GPUdbException( ex_str )
        # end try
    # end get_records_by_key

# end class RecordRetriever
