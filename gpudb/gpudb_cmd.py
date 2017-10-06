#!/usr/bin/env python

# ######################################################
# 
# Command-line python interface to GPUdb
#
# @file run_gpudb.py
# @author Meem Mahmud
# ######################################################

from __future__ import print_function

from gpudb import GPUdb

import os
import sys
import argparse
import json

if sys.version_info.major > 2:
    long = int

from avro import schema

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def gpudb_cmd( argv ):
    """A command line interface to send a specified request to a GPUDB server.
       Can be used to print the parameters for a request as well.
    """

    # Default values
    file_name = ""

    # Add arguments to the parser
    parser = argparse.ArgumentParser()
    parser.add_argument( '-g', '--gpudb', nargs = '?', default = "127.0.0.1:9191",
                         help = "IP address and port of GPUdb in the format: IP_ADDRESS:PORT (default 127.0.0.1:9191)" )
    parser.add_argument( '--json-encoding', action = "store_true",
                         help = "Use avro JSON encoding of request message to GPUdb (default is avro binary)" )
    parser.add_argument( '-f', '--format', action = 'store', dest = "format", default = "json",
                         choices = ["json", "oneline", "ini", "raw"],
                         help = "Format the returned GPUDB response in a few ways. (default 'json')" )
    parser.add_argument( "--print-query", action = 'store_true',
                         help = "Print the request query before sending it using the specified format." )

    # User must provide one or the other
    query_group = parser.add_mutually_exclusive_group( required = True )
    query_group.add_argument( "--list-queries", action = 'store_true',
                         help = "Lists all available GPUDB request queries." )
    query_group.add_argument( "--print-schemas", action = 'store',
                         help = "Print the JSON schema of the specified request and response query." )
    query_group.add_argument( '--query', nargs = argparse.REMAINDER,
                         help = "Send a request query by specifying the name of the query and the parameters associated with the query. " \
                                "Help is provided if only the query name is specified. " \
                                "Note that unspecified parameters will take a default value. " \
                                "Example: '--query aggregate_min_max --column_name x --table_name DataTable'" )

    # Print the help message and quit if no arguments are given
    if ( len(sys.argv) == 1 ): # None provided
        parser.print_help()
        sys.exit( 2 )

    # Parse the command line arguments
    args = parser.parse_args()


    # --------------------------------------
    # Set up GPUdb
    GPUdb_IP, GPUdb_Port = args.gpudb.split( ":" )
    encoding = 'JSON' if args.json_encoding else 'BINARY'
    gpudb = GPUdb( encoding = encoding, host = GPUdb_IP, port = GPUdb_Port )

    # Get a list of all endpoint names
    query_names = sorted( gpudb.gpudb_schemas.keys() )

    # --------------------------------------
    # Only print the request and response schemas is asked to
    if args.print_schemas :
        query_name = args.print_schemas
        if query_name not in query_names:
            print("Unknown query name: '%s'" % query_name)
            sys.exit( 2 )

        req_schema_str = gpudb.gpudb_schemas[ query_name ][ "REQ_SCHEMA_STR" ]
        rsp_schema_str = gpudb.gpudb_schemas[ query_name ][ "RSP_SCHEMA_STR" ]
        req_odict = json.JSONDecoder(object_pairs_hook=collections.OrderedDict).decode(req_schema_str)
        rsp_odict = json.JSONDecoder(object_pairs_hook=collections.OrderedDict).decode(rsp_schema_str)

        # Use desired formatting
        print_dict( req_odict, args.format )
        print_dict( rsp_odict, args.format )
        sys.exit(0)

    # --------------------------------------
    # List all endpoint/query names, if desired by user
    if (args.list_queries == True) or (len(args.query) == 0):
        for q in sorted( query_names ):
            print(q)
        sys.exit( 0 ) # Succesful termination after printing the desired help message

    # --------------------------------------
    # Get the query JSON string from GPUdb
    query_name = args.query[ 0 ]
    if query_name not in query_names:
        print("Unknown query name: '%s'" % query_name)
        sys.exit( 2 )
    request_json = gpudb.gpudb_schemas[ query_name ][ "REQ_SCHEMA_STR" ]

    # Parse the request JSON to get the parameters
    request_schema = gpudb.gpudb_schemas[ query_name ][ "REQ_SCHEMA" ]
    request_json =  request_schema.to_json()["fields"]

    # Create a dictionary of (param name, param type) pairs based on the JSON
    param_name_type = {}
    param_vals = {}
    for param in request_json:
        param_name_type[ param['name'] ] = param['type']
        # Binary/bytes parameters will be skipped
        if param['type'] == "string" or param['type'] == "bytes":
            param_vals[ param['name'] ] = "" # Default is empty string
        if param['type'] == "map":
            param_vals[ param['name'] ] = {} # Default is empty map
        if param['type'] == "list":
            param_vals[ param['name'] ] = [] # Default is empty list
        # Note that numeric attributes are not getting a default
        # User MUST provide such values, or we output an error

    # Create a parser for query-specific parameters
    query_parser = argparse.ArgumentParser()

    # Add parameters to be parsed
    for pname, ptype in param_name_type.items():
        if ptype == "string": # Make string arguments optional
            query_parser.add_argument( "--" + pname, nargs='?', default="", help = "Defaults to empty string" )
        elif ptype == "double" or ptype == "float":
            query_parser.add_argument( "--" + pname, type = float, required = True, help = "Required parameter, type %s" % ptype )
        elif ptype == "long":
            query_parser.add_argument( "--" + pname, type = long, required = True, help = "Required parameter, type %s" % ptype )
        elif ptype == "int":
            query_parser.add_argument( "--" + pname, type = int, required = True, help = "Required parameter, type %s" % ptype )
        elif ptype == "bytes":
            continue # ignore bytes
        elif ptype == "boolean": # Boolean flag
            # User must provide one or the other
            bool_group = query_parser.add_mutually_exclusive_group( required = True )
            bool_group.add_argument( "--" + pname, action = 'store_true', dest = pname,
                                       help = "Boolean parameter, include to set %s to TRUE" %pname )
            bool_group.add_argument( "--no-" + pname, action = 'store_false', dest = pname,
                                       help = "Boolean parameter, include to set %s to FALSE" % pname )
        else: # Maps and lists get empty ones by default; handling is delicate; ignore 'bytes'
            if ptype[ 'type' ] == "map":
                query_parser.add_argument( "--" + pname, nargs = '?', type = json.loads, default = {},
                                           help = "Expected map value of type: %s; surround the whole map with single quotes (') and any string (key or value) within with double quotes (\"). E.g. for random, --param_map '{\"x\":{\"min\":2}}'. When omitted, defaults to empty map" % ptype['values'] )
            else: # Arrays
                query_parser.add_argument( "--" + pname, type = json.loads, default=[],
                                           help = "Comma separated list (escape spaces with \) enclosed in []. For example, for filter_by_nai, --x_vector [1,2,3,4] or --x_vector [1,\ 2,\ 3,\ 4]. If contains strings, then enclose the whole thing within single quotes and the individual string in double quotes.  E.g., for filter_by_string, --attributes '[\"x\",\"y\"]'. When omitted, defaults to an empty list." )

    # Print the help message and quit if no arguments are given (and none is expected)
    if ( len( args.query[1:] ) == 0 and len( param_name_type ) > 0 ):
        print("No parameters provided for query: ", query_name)
        query_parser.print_help()
        sys.exit( 2 )

    # Parse the parameters and store in a dictionary
    query_args = vars( query_parser.parse_args( args.query[1:] ) )

    # Copy the parsed values to the ordered dictionary to pass to GPUdb
    for key, val in query_args.items():
        param_vals[ key ] = val

    # --------------------------------------
    # Call the GPUDB query:

    # Obtain the request and response schemas for the given query
    (req_schema, resp_schema) = gpudb._GPUdb__get_schemas( query_name )
    endpoint = gpudb._GPUdb__get_endpoint( query_name )

    # --------------------------------------
    if args.print_query :
        encoded_datum = gpudb.encode_datum(req_schema, param_vals)
        request_odict = gpudb._GPUdb__read_orig_datum(req_schema, encoded_datum)
        print(endpoint)
        print_dict( request_odict, args.format )

    # --------------------------------------
    # Perform the GPUDB query
    response = gpudb._GPUdb__post_then_get( req_schema, resp_schema, param_vals, endpoint )

    print_dict( response, args.format )
    # --------------------------------------

# end gpudb_cmd


def print_dict( response, format_type ):
    if format_type == "oneline":
        print(json.dumps(response))
    elif format_type == "ini":
        print(format_response( response ))
    elif format_type == "raw" :
        # prints OrderedDict(..), note that pprint doesn't do anything different
        print(response)
    else:
        print(json.dumps(response, indent=4))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def format_response( response, num_tabs = 0 ):
    """Format the gpudb response prettily for printing to screen
    """
    output = ""
    spaces = "    "

    for key, val in response.items():
        # Embedded map
        if isinstance( val, dict ):
            output += num_tabs * spaces + str(key) + ":\n"
            output += format_response( val, num_tabs + 1 )
        elif isinstance( val, list ):
            output += num_tabs * spaces + str(key) + ":\n"
            num_tabs += 1
            for val_item in val: # iterate over the list
                if isinstance( val_item, dict ):
                    output += format_response( val_item, num_tabs )
                else: # 
                    output += num_tabs * spaces + str(val_item) + "\n"
        else: # regular (key, val) pair => val is a scalar datatype
            output += num_tabs * spaces + "%s: %s\n" % (key, val)

    return output

# end format_response



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    gpudb_cmd( sys.argv )
