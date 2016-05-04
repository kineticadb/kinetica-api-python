###############################################################################
#
# gpudb_ingestor.py
#
# Python API file for inserting multiple records into GPUdb via one or more
# nodes/heads.
#
# Copyright (c) 2016 GIS Federal
#
###############################################################################


from avro import schema, datafile, io
import cStringIO, StringIO
import json
import mmh3 # murmur hash
import random
import re
import sys

if sys.version_info >= (2, 7):
    import collections
else:
    import ordereddict as collections # a separate package


from gpudb import GPUdb


# Some string constants used throughout the program
class C:
    """Some string constants used throughout the program."""

    # JSON dict keys
    _name   = "name"
    _type   = "type"
    _fields = "fields"

    # gpudb response status related dict keys
    _info   = "status_info"
    _msg    = "message"
    _status = "status"
    _error  = "ERROR"
    _ok     = "OK"

    # GPUdb /admin/get/shardassignments response dict keys
    _shard_ranks = "shard_assignments_rank"

    # GPUdb /insert/records response dict keys
    _count_inserted = "count_inserted"
    _count_updated  = "count_updated"

    # GPUdb /show/table response dict keys
    _table_names = "table_names"
    _table_properties  = "properties"
    _is_collection = "is_collection"
    _type_schemas  = "type_schemas"
    _pk        = "primary_key"
    _shard_key = "shard_key"

    # GPUdb /system/properties response dict keys
    _sys_properties    = "property_map"
    _multihead_enabled = "conf.enable_worker_http_servers"
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




class GPUdbIngestor:
    """
    """

    # Exception: InsertionException
    # =============================
    class InsertionException(Exception):
        """Handles errors during insertion of records into GPUdb.
        """
        def __init__( self, message, records ):
            # call the base class constructor
            super(GPUdbIngestor.InsertionException, self).__init__( message )

            self.records = records
        # end __init__

        def get_records( self ):
            return self.records
        # end get_records
    # end class InsertionException


    # Inner Class WorkerList
    # ======================

    class WorkerList:
        """A list of worker URLs to use for multi-head ingest."""

        def __init__( self, gpudb, ip_regex = "" ):
            """Automatically populates the WorkerList object with the worker
            URLs for the GPUdb server to support multi-head ingest. (If the
            specified GPUdb instance has multi-head ingest disabled, the worker
            list will be empty and multi-head ingest will not be used.) Note
            that in some cases, workers may be configured to use more than one
            IP address, not all of which may be accessible to the client; this
            constructor uses the first IP returned by the server for each
            worker.

            @param gpudb  The GPUdb client handle from which to obtain the
                          worker URLs.
            @param ip_regex  Optional IP regular expression to match for the
                             worker URLs.
            """
            # Check the input parameter type
            assert isinstance(gpudb, GPUdb), ("Parameter 'gpudb' must be of "
                                              "type GPUdb; given %s"
                                              % type(gpudb) )

            self.worker_urls = []

            # Get system properties
            system_prop_rsp = gpudb.show_system_properties()
            if system_prop_rsp[ C._info ][ C._status ] == C._error:
                raise ValueError( "Unable to retrieve system properties; error:"
                                  " %s" % system_prop_rsp[ C._info ][ C._msg ] )

            system_properties = system_prop_rsp[ C._sys_properties ]

            # Is multi-head ingest enabled on the server?
            if C._multihead_enabled not in system_properties:
                raise ValueError( "Missing value for %s" % C._multihead_enabled)

            self.multihead_enabled = (system_properties[ C._multihead_enabled ] == C._TRUE)
            if not self.multihead_enabled:
                # Multihead ingest is not enabled.  Just return the main/only ingestor
                self.worker_urls.append( gpudb.get_url() )
                return # nothing to do

            # Get the worker IP addresses (per rank)
            if C._worker_IPs not in system_properties:
                raise ValueError( "Missing value for %s" % C._worker_IPs)

            self.worker_IPs_per_rank = system_properties[ C._worker_IPs ].split( ";" )

            # Get the worker ports
            if C._worker_ports not in system_properties:
                raise ValueError( "Missing value for %s" % C._worker_ports)

            self.worker_ports = system_properties[ C._worker_ports ].split( ";" )

            # Check that the IP and port list lengths match
            if (len(self.worker_IPs_per_rank) != len(self.worker_ports)):
                raise ValueError("Inconsistent number of values for %s and %s."
                                 % (C._worker_IPs_per_rank, C._worker_ports) )

            # Process the IP addresses per rank
            for i in range(0, len(self.worker_IPs_per_rank)):
                ip_address = self.worker_IPs_per_rank[ i ]
                # ips_per_rank = self.worker_IPs_per_rank[ i ]
                found = False

                # Validate the IP address's syntax
                if not self.validate_ip_address( ip_address ):
                    raise ValueError( "Malformed IP address: %s" % ip_address )

                # Generate the URL using the IP address and the port
                # url = (ip_address + ":" + self.worker_ports[i])
                url = ("http://" + ip_address + ":" + self.worker_ports[i])

                if (ip_regex == ""): # no regex given
                    # so, include all IP addresses
                    self.worker_urls.append( url )
                    found = True
                else: # check for matching regex
                    match = re.match(ip_regex, ip_address)
                    if match: # match found
                        self.worker_urls.append( url )
                        found = True
                        # skip the rest of IP addresses for this rank
                        continue
                    # end found match
                # end if-else

                # if no worker found for this rank, throw exception
                if not found:
                    raise ValueError("No matching IP address found for worker"
                                     "%d." % i)
            # end outer loop of processing worker IPs

            # if no worker found, throw error
            if not self.worker_urls:
                raise ValueError( "No worker HTTP servers found." )
        # end WorkerList __init__


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
            """Return a list of the URLs for the GPUdb workers."""
            return self.worker_urls
        # end get_worker_urls

    # end class WorkerList


    # Inner Class RecordKey
    # =====================
    class RecordKey:
        """Represents a record key for ingestion jobs to GPUdb."""

        # Member variables:
        # -----------------
        # The hash value for this record key (used internally in the python API)
        hash_code = 0

        # The hash value for routing the record to the appropriate GPUdb worker
        routing_hash = 0


        def __init__( self, buffer_size ):
            """Initialize the RecordKey (need a builder?)
            """
            self.record_key = {}
            self.buffer_size  = buffer_size
            self.buffer_value = bytearray()
        # end RecordKey __init__


        def add_char( self, value ):
            """Adds some characters to the record key byte array."""
            self.buffer_value.append( bytearray( value ) )
        # end add_char

        def add_number( self, value ):
            """Adds numeric value to the record key byte array."""
            self.buffer_value.append( bytearray( value ) )
        # end add_char


        def add_string( self, value ):
            """Adds a string to the record key byte array."""
            string_hash = mmh3.hash_bytes( value )
            self.buffer_value.append( bytearray( string_hash ) )
        # end add_char


        def compute_hashes( self ):
            """Compute the Murmur hash of the key.
            """
            self.routing_hash = mmh3.hash_bytes( self.buffer_value )
            self.hash_code = int( self.routing_hash ^ (self.routing_hash >> 32) )
        # end compute_hashes


        def route( self, routing_table ):
            """Given a routing table, return the rank of the GPUdb server that
            this record key should be routed to.

            @param routing_table  A list of integers...

            @returns the rank of the GPUdb server that this record key should be
                     routed to.
            """
            routing_index = ((self.routing % len(routing_table) ) - 1)
            return routing_table[ routing_index ]
        # end route
    # end class RecordKey



    # Inner Class RecordKeyBuilder
    # ============================
    class RecordKeyBuilder:
        """Creates RecordKey objects given a particular kind of table schema.
        """
        column_type_sizes = collections.OrderedDict()
        column_type_sizes[ "char1" ] =  1
        column_type_sizes[ "char2" ] =  2
        column_type_sizes[ "char4" ] =  4
        column_type_sizes[ "char8" ] =  8
        column_type_sizes[ "char16"] = 16
        column_type_sizes[ "double"] =  8
        column_type_sizes[ "float" ] =  4
        column_type_sizes[ "int"   ] =  4
        column_type_sizes[ "int8"  ] =  1
        column_type_sizes[ "int16" ] =  2
        column_type_sizes[ "long"  ] =  8
        column_type_sizes[ "string"] =  8

        def __init__( self,
                      gpudb,
                      table_name,
                      has_primary_key = False ):
            """Initializes a RecordKeyBuilder object.
            """
            # Check the input parameter type 'gpudb'
            assert isinstance(gpudb, GPUdb), ("Parameter 'gpudb' must be of "
                                              "type GPUdb; given %s"
                                              % type(gpudb) )
            # Validate input parameter 'table_name'
            assert isinstance(table_name, str), ("Parameter 'table_name' must be a"
                                                 "string; given %s"
                                                 % type(table_name) )
            # Validate the boolean parameters
            if has_primary_key not in [True, False]:
                raise ValueError( "Constructor parameter 'has_primary_key' must be a "
                                  "boolean value; given: %s" % has_primary_key )


            # Get the table type schema from GPUdb
            show_table_rsp = gpudb.show_table( table_name )
            assert (show_table_rsp[ C._info ][ C._status ] == C._ok), \
                show_table_rsp[ C._info ][ C._msg ]
            assert (show_table_rsp[ C._table_names ][ 0 ] == table_name), \
                ("Table name doesn't match; given: '%s', received '%s'"
                 "" % (table_name, show_table_rsp[ C._table_names ][ 0 ]))
            assert (not show_table_rsp[ C._is_collection ][ 0 ]), \
                ("Cannot instantiate a RecordKeyBuilder for a 'collection' type"
                 " table; table name: '%s'" % table_name)
            table_schema = show_table_rsp[ C._type_schemas ][ 0 ]
            table_schema_properties = show_table_rsp[ C._table_properties ][ 0 ]

            # Save the table schema related information
            self.table_name    = table_name
            self.table_schema  = json.loads( table_schema )
            self.table_columns = self.table_schema[ C._fields ]
            self.table_column_names = [col[ C._name ] for col in self.table_columns]
            self.column_properties = table_schema_properties

            # A list of which columns are primary/shard keys
            self.pk_shard_key_indices = []
            self.key_columns_names = []
            self.key_schema_fields = collections.OrderedDict()
            
            # Go over all columns and see which ones are primary or shard keys
            for i in range(0, len(self.table_columns)):
                column_name = self.table_column_names[ i ]
                column_type = self.table_columns[ i ][ C._type ]
                column_properties = self.column_properties[ column_name ]

                is_key = False
                # Check for primary keys, if any
                if has_primary_key and (C._pk in column_properties):
                    is_key = True
                elif ( (not has_primary_key)
                       and (C._shard_key in column_properties) ):
                    # turned out to be a shard key
                    is_key = True

                # Save the key index
                if is_key:
                    # Some additional checks on keys before saving the index
                    if "ipv4" in column_properties:
                        raise ValueError( "Cannot use '%s' as a key."  % column_name)
                    if column_type in ["bytes"]:
                        raise ValueError( "Cannot use '%s' as a key."  % column_name)

                    # ok to use as a key
                    self.pk_shard_key_indices.append( i )
                    self.key_columns_names.append( column_name )
                    # Build the key schema fields
                    self.key_schema_fields[ C._name ] = column_name
                    self.key_schema_fields[ C._type ] = column_type
            # end loop over columns

            # Check if it's a track-type table
            track_type_special_columns = set(["TRACKID", "TIMESTAMP", "x", "y"])
            is_track_type = track_type_special_columns.issubset( self.table_column_names )
            if ((not has_primary_key) and is_track_type):
                track_id_index = self.table_column_names.index( "TRACKID" )
                if not self.pk_shard_key_indices: # no pk/shard key found yet
                    self.pk_shard_key_indices.append( track_id_index )
                elif ( (len( self.pk_shard_key_indices ) != 1)
                       or (self.pk_shard_key_indices[0] != track_id_index ) ):
                    raise ValueError( "Cannot have a shard key other than "
                                      "'TRACKID' for track-type tables." )
            # end checking track-type tables

            self.key_buffer_size = 0
            if not self.pk_shard_key_indices: # no pk/shard key found
                return
            # end no pk/shard key for this table

            # Calculate the buffer size for this type of objects/records
            # with the given primary (and/or) shard keys
            self.key_types = []
            for i in self.pk_shard_key_indices:
                column_name = self.table_column_names[ i ]
                column_type = self.table_columns[ i ][ C._type ]
                column_properties = self.column_properties[ i ]

                # Check for any property related to data types
                type_related_properties = set( column_properties ).intersection( column_type_sizes )
                type_related_properties = list( type_related_properties )
                if type_related_properties:
                    # Some special property found related to the data type
                    # Check that only one type-related property found
                    assert (len(type_related_properties) == 1), \
                        ("Column '%s' has multiple type-related properties "
                         "(must have at most one): %s"
                         "" % (column_name, str( type_related_properties ) ) )
                    # Use the special property and its size for the data type
                    column_type = type_related_properties[ 0 ]
                    self.key_buffer_size += self.column_type_sizes[ column_type ]
                    self.key_types.append( column_type )
                else: # no type-related property found
                    # So, the primitive type and its size are used
                    self.key_buffer_size += self.column_type_sizes[ column_type ]
                    self.key_types.append( column_type )
                # end if-else
            # end loop


            # Build the key schema
            self.key_schema_fields_str = [ '{"name":"%s", "type":"%s"}' % (k, v)
                                           for k,v in self.key_schema_fields.iteritems() ]
            self.key_schema_str = ("""{ "type" : "record",
                                       "name" : "",
                                       "fields" : [%s] }""" \
                                           % ",".join( self.key_schema_fields_str)).replace(" ", "").replace("\n","")
            self.key_schema = schema.parse( self.key_schema_str )
        # end RecordKeyBuilder __init__



        def build( self, record ):
            """Builds a RecordKey object based on the input data and returns it.

            @param record  An object of the given type to make the record key
                           out of.
            """
            # Nothing to do if the key size is zero!
            if (self.key_buffer_size == 0):
                return None

            # Check that the given record is a dict of the given table
            # type
            if not isinstance( record, dict ):
                raise ValueError( "Given record must be a dict; given %s"
                                  % type( record ) )
            # Check all the keys of the given record
            record_keys = record.keys()
            if (record_keys != self.table_column_names):
                raise ValueError( "Given record must be of the type for GPUdb table '%s'"
                                  " (with columns '%s'); given record has columns '%s' "
                                  % (self.table_name,
                                     self.table_column_names,
                                     record_keys) )

            # Create and populate a RecordKey object
            record_key = RecordKey( self.key_buffer_size )
            for i in range(0, len(self.key_columns_names)):
                # get the key, value pair
                key   = self.key_columns_names[ i ]
                value = record[ key ]
                key_type = self.key_types[ i ]

                # Add to the record key
                if key_type in ["char1", "char2", "char4", "char8", "char16"]:
                    record_key.add_char( value )
                elif key_type in ["double", "float", "int", "int8", "int16", "long"]:
                    record_key.add_number( value )
                elif key_type in ["string"]:
                    record_key.add_string( value )
                else:
                    raise ValueError( "Unknown key type given: '%s'" % key_type )
            # end loop

            # Compute the key hash and return the key
            record_key.compute_hashes()
            return record_key
        # end build()


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

    # end class RecordKeyBuilder #########################



    # Inner Class WorkerQueue
    # =======================
    class WorkerQueue:
        """Maintains a queue for the worker nodes/ranks of the GPUdb server.
        
        """
        def __init__( self,
                      url = "127.0.0.1:9191",
                      gpudb = None,
                      capacity = 10000,
                      has_primary_key = False,
                      update_on_existing_pk = False ):
            """Sets up the ...
            """
            # Validate input parameter 'gpudb'
            assert isinstance(gpudb, GPUdb), ("Parameter 'gpudb' must be of "
                                              "type GPUdb; given %s"
                                              % type(gpudb) )
            # Validate the URL???
            # Validate the capacity
            if (capacity <= 0):
                raise ValueError( "Constructor parameter 'capacity' must be a"
                                  "non-zero positive value; given: %d" % capacity )
            # Validate the boolean parameters
            if has_primary_key not in [True, False]:
                raise ValueError( "Constructor parameter 'has_primary_key' must be a "
                                  "boolean value; given: %s" % has_primary_key )
            if update_on_existing_pk not in [True, False]:
                raise ValueError( "Constructor parameter 'update_on_existing_pk' must be a "
                                  "boolean value; given: %s" % update_on_existing_pk )

            url = str( url ) # in case it's unicode

            # Save the values
            self.url = url
            self.capacity = capacity
            self.has_primary_key = has_primary_key
            self.update_on_existing_pk = update_on_existing_pk

            # Create a gpudb instance
            worker_host, sep, worker_port = url.rpartition( ":" )
            self.gpudb = GPUdb( host = worker_host,
                                port = int(worker_port),
                                encoding = gpudb.encoding,
                                connection = gpudb.connection, 
                                username = gpudb.username,
                                password = gpudb.password )

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
            """Insert the data to be inserted (into the relevant GPUdb table)
            into the queue.
            """
            old_queue_length = len( self.record_queue )

            if self.has_primary_key:
                # table has primary key
                if self.update_on_existing_pk:
                    # update on existing primary key (if key exists)
                    if key not in self.primary_key_to_queue_index_map:
                        # the key doesn't exist; add it to the queue and
                        # keep track of the queue index in the key->index map
                        self.record_queue.append( record )
                        self.primary_key_to_queue_index_map[ key ] = old_queue_length
                    else: # key already exists
                        # find the index for this key in the record queue
                        key_index = self.primary_key_to_queue_index_map[ key ]
                        self.record_queue[ key_index ] = record
                    # done updating on (existing) primary key
                else: # if key already exists, do NOT insert this record
                    if key in self.primary_key_to_queue_index_map:
                        # yes, the key exists, so, it's a problem
                        return None
                    else: # key does not already exist
                        self.record_queue.append( record )
                        self.primary_key_to_queue_index_map[ key ] = old_queue_length
                # end update on existing PK if-else
            else:
                # the table has no primary key; so no map to worry about
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


        def get_gpudb( self ):
            """Return the GPUdb handle for this worker."""
            return self.gpudb
        # end get_gpudb

    # end class WorkerQueue



    # GPUdbIngestor Methods
    # =====================

    def __init__( self,
                  gpudb,
                  table_name,
                  batch_size,
                  options = None,
                  workers = None ):
        """Initializes the GPUdbIngestor instance.

        @param gpudb
        @param table_name
        @param batch_size
        @param options
        @param workers
        """

        # Validate input parameter 'gpudb'
        assert isinstance(gpudb, GPUdb), ("Parameter 'gpudb' must be of "
                                          "type GPUdb; given %s"
                                          % type(gpudb) )
        # Validate input parameter 'table_name'
        assert isinstance(table_name, str), ("Parameter 'table_name' must be a"
                                             "string; given %s"
                                             % type(table_name) )
        # Validate input parameter 'batch_size'
        assert (isinstance(batch_size, int)
                and (batch_size >= 1)), ("Parameter 'batch_size' must be greater"
                                         " than zero; given %d" % batch_size )
        # Validate input parameter 'options'
        assert isinstance(options, (dict, None)), ("Parameter 'options' must be a"
                                                   "dicitonary, if given; given %s"
                                                   % type(options) )
        # Validate input parameter 'workers'
        assert ((not workers) or isinstance(workers, self.WorkerList)), \
            ("Parameter 'workers' must be of type WorkerList; given %s"
             % type(workers) )
        # Save the parameter values
        self.gpudb      = gpudb
        self.table_name = table_name
        self.batch_size = batch_size
        self.options    = options

        self.count_inserted = 0
        self.count_updated  = 0

        # Get the schema for the records for this table from GPUdb
        show_table_rsp = self.gpudb.show_table( table_name )
        assert (show_table_rsp[ C._info ][ C._status ] == C._ok), \
            show_table_rsp[ C._info ][ C._msg ]
        assert (show_table_rsp[ C._table_names ][ 0 ] == table_name), \
            ("Table name doesn't match; given: '%s', received '%s'"
             "" % (table_name, show_table_rsp[ C._table_names ][ 0 ]))
        assert (not show_table_rsp[ C._is_collection ][ 0 ]), \
            ("Cannot instantiate a RecordKeyBuilder for a 'collection' type"
             " table; table name: '%s'" % table_name)
        record_schema_str = show_table_rsp[ C._type_schemas ][ 0 ]
        self.record_schema = schema.parse( record_schema_str )

        # Create the primary and shard key builders
        shard_key_builder   = self.RecordKeyBuilder( self.gpudb, self.table_name )
        primary_key_builder = self.RecordKeyBuilder( self.gpudb, self.table_name,
                                                     has_primary_key = True )
        self.primary_key_builder = None
        self.shard_key_builder = None
        # Save the appropriate key builders
        if primary_key_builder.has_key():
            self.primary_key_builder = primary_key_builder

            # If both pk and shard keys exist; check that they're not the same
            if (shard_key_builder.has_key()
                and (not shard_key_builder.has_same_key( primary_key_builder ))):
                self.shard_key_builder = shard_key_builder
        elif shard_key_builder.has_key():
            self.shard_key_builder = shard_key_builder
        # end saving the key builders


        # Boolean flag for primary key related info
        update_on_existing_pk = False
        if ( self.options
             and ("update_on_existing_pk" in self.options) ):
            update_on_existing_pk = (self.options[ "update_on_existing_pk" ] == "true")
        # end if

        self.worker_queues = []

        # Create a list of worker queues and a map of url to GPUdb worker ranks
        if not workers: # but no worker provided
            worker_url = self.gpudb.get_url()
            try:
                has_primary_key = (self.primary_key_builder != None)
                wq = self.WorkerQueue( worker_url,
                                       self.gpudb,
                                       self.batch_size,
                                       has_primary_key,
                                       update_on_existing_pk )
                self.worker_queues.append( wq )
            except ValueError as e:
                raise
        else: # workers provided
            for worker in workers.get_worker_urls():
                try:
                    worker_url = worker
                    wq = self.WorkerQueue( worker_url, self.gpudb,
                                           self.batch_size, update_on_existing_pk )
                    self.worker_queues.append( wq )

                    # Create a gpudb per worker
                    worker_host, sep, worker_port = worker.rpartition( ":" )
                    # worker_host, worker_port = worker.split( ":" )
                except ValueError as e:
                    raise
            # end loop over workers
        # end if-else

        # Get the number of workers
        if not workers:
            self.num_ranks = 1
        else:
            self.num_ranks = len( workers.get_worker_urls() )

        self.routing_table = None
        if ( (self.num_ranks > 1)
             and (self.primary_key_builder or self.shard_key_builder) ):
            # Get the sharding assignment ranks
            shard_info = self.gpudb.adming_get_shard_assignments()
            self.routing_table = shard_info[ C._shard_ranks ]
        # end if
    # end GPUdbIngestor __init__


    def get_gpudb( self ):
        """Return the instance of GPUdb client used by this ingestor."""
        return self.gpudb
    # end get_gpudb


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


    def flush( self ):
        """Ensures that any queued records are inserted into GPUdb. If an error
        occurs while inserting the records from any queue, the records will no
        longer be in that queue nor in GPUdb; catch {@link InsertException} to
        get the list of records that were being inserted if needed (for example,
        to retry). Other queues may also still contain unflushed records if
        this occurs.

        @throws InsertException if an error occurs while inserting records.
        """
        for worker in self.worker_queues:
            # do we need to synchronize here?
            queue = worker.flush()
            self.__flush( queue, worker.get_gpudb() )
    # end flush



    def __flush( self, queue, worker_gpudb ):
        """Internal method to flush--actually insert--the records to GPUdb.

        @param queue  List of records to insert
        @param url  The URL to which to send the records.
        """
        if not queue:
            return # nothing to do

        try:
            print "Flushing to %s with %d objects" % (worker_gpudb.get_url(), len(queue)) # debug~~~~~~~~~
            # Insert the records
            insert_rsp = worker_gpudb.insert_records( table_name = self.table_name,
                                                      data = queue,
                                                      options = self.options )
            self.count_inserted += insert_rsp[ C._count_inserted ]
            self.count_updated  += insert_rsp[ C._count_updated  ]
            print "insert status:", insert_rsp[ C._info ][ C._status ], "self.count_inserted:", self.count_inserted, "self.count_updated:", self.count_updated # debug~~~~~~~~~~
        except Exception as e:
            raise self.InsertionException( str(e), queue )
    # end __flush



    def insert_record( self, record, record_encoding = "json" ):
        """Queues a record for insertion into GPUdb. If the queue reaches the
        {@link #get_batch_size batch size}, all records in the queue will be
        inserted into GPUdb before the method returns. If an error occurs while
        inserting the records, the records will no longer be in the queue nor in
        GPUdb; catch {@link InsertionException} to get the list of records that were
        being inserted if needed (for example, to retry).

        @param record  The record to insert.

        @throws InsertionException if an error occurs while inserting.
        """
        assert isinstance(record, collections.OrderedDict), \
            "Input parameter 'record' must be an OrderedDict; given %s" % type(record)

        assert record_encoding in ("json", "binary"), \
            "Input parameter 'record_encoding' must be one of ['json', 'binary']; given '%s'" % record_encoding

        # Build the primary and/or shard key(s) for this record
        primary_key = None
        shard_key   = None

        # Build the primary key
        if self.primary_key_builder:
            primary_key = self.primary_key_builder.build( record )

        # Build the shard key
        if self.shard_key_builder:
            shard_key = self.shard_key_builder.build( record )
        else: # use the primary key, if any
            shard_key = primary_key

        # Create a worker queue
        worker_queue = None
        # Get the index of the worker to be used
        if (not shard_key) and (not self.routing_table):
            if (self.num_ranks == 1):
                # only one worker available
                worker_index = 0
            else:  # get a random worker
                worker_index = random.randint( 0, (self.num_ranks - 1) )
        else:
            # Use the routing table and the shard key to find the right worker
            worker_index = shard_key.route( self.routing_table )
        # Get the worker
        worker_queue = self.worker_queues[ worker_index ]

        # Encode the object into binary if not already encoded
        if record_encoding == "json":
            encoded_record = self.gpudb.write_datum( self.record_schema, record )
        else:
            encoded_record = record


        # Insert the record for the worker queue
        # -----------any synchronization needed??-----------------
        queue = worker_queue.insert( encoded_record, primary_key )

        # Flush, if necessary (when the worker queue returns a non-empty queue)
        if queue:
            self.__flush( queue, worker_queue.get_gpudb() )
    # end insert


    def insert_records( self, records, record_encoding = "json" ):
        """Queues a list of records for insertion into GPUdb. If any queue reaches
        the {@link #get_batch_size batch size}, all records in that queue will be
        inserted into GPUdb before the method returns. If an error occurs while
        inserting the queued records, the records will no longer be in that queue
        nor in GPUdb; catch {@link InsertionException} to get the list of records
        that were being inserted (including any from the queue in question and
        any remaining in the list not yet queued) if needed (for example, to
        retry). Note that depending on the number of records, multiple calls to
        GPUdb may occur.

        @param records  the records to insert

        @throws InsertionException if an error occurs while inserting
        """
        assert isinstance(records, list), \
            "Input parameter 'records' must be a list; given %s" % type(records)

        assert record_encoding in ("json", "binary"), \
            "Input parameter 'record_encoding' must be one of ['json', 'binary']; given '%s'" % record_encoding

        for record in records:
            try:
                self.insert_record( record, record_encoding )
            except self.InsertionException as e:
                # Add the remaining records that could not be inserted
                uninserted_records = e.get_records()
                for rec in records[ records.index( record ) :  ]:
                    uninserted_records.append( rec )
                # done adding un-inserted records

                raise
            # done handling the error case
    # end insert

# end class GPUdbIngestor


























