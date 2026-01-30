#!/usr/bin/python

# ######################################################
# 
# Script to run a diagnostic test on GPUdb
#
# @file gpudb_diagnostics.py
# @author Meem Mahmud
# ######################################################

from __future__ import print_function


from gpudb import GPUdb
import datetime
import sys
import getopt




helpMessage = """Usage:
  -u <URL> -- Run script on given URL
  -U <username> -- Run script with the given username
  -P <password> -- Run script with the given password
  -l -- Run script on local GPUdb (127.0.0.1:9191) (lower case L)
        (This is default if neither -l nor -g is specified)
  -g ###.##.##.## -- Run script on server GPUdb @ the IP address provided
  -p #### -- Run script on  the specified port (default is 9191)
  -v -- Prints verbose messages
  -h -- Print this help message
"""


def diagnose_gpudb( argv ):
    """
    Run a diagnostic test on GPUdb
    Argument:
      argv -- Command line arguments
    """

    def get_prefix_table_name_with_current_datetime( prefix ):
        # Get a table name without a period (not allowed in 7.1)
        return "{prefix}_{now}".format( prefix = prefix,
                                        now = datetime.datetime.now()
                                                  .isoformat()
                                                  .split(".")[0] )
    # end get_prefix_table_name_with_current_datetime


    # Parse the command line arguments
    if ( len(sys.argv) == 1 ): # None provided
        # Print help message and quit
        print ( helpMessage )
        sys.exit( 2 )
    try: # Parse the command line arguments
        opts, args = getopt.getopt( sys.argv[1:], "hu:U:P:lg:p:v" )
    except getopt.GetoptError:
        print ( helpMessage )
        sys.exit( 2 )

    # Some default values
    GPUdb_URL  = None
    GPUdb_User = None
    GPUdb_Pass = None
    GPUdb_IP   = '127.0.0.1' # Run locally by default
    GPUdb_Port = '9191' # Default port
    isVerbose  = False

    # Parse the arguments
    for opt, arg in opts:
        if opt == '-h': # print usage and exit
            print ( helpMessage )
            sys.exit()
        if opt == '-u': # run gpudb on a server gpudb at the specified URL
            GPUdb_URL = arg
        if opt == '-U': # run gpudb on a server gpudb with the specified username
            GPUdb_User = arg
        if opt == '-P': # run gpudb on a server gpudb with the specified password
            GPUdb_Pass = arg
        if opt == '-l': # run gpudb on local machine
            isServer = False
        if opt == '-g': # run gpudb on a server gpudb at the specified IP address
            GPUdb_IP = arg
            set_id = "TwitterPointText" # Default set ID for server gpudb
        if opt == '-p': # run gpudb on a server gpudb at the specified port
            GPUdb_Port = arg
        if opt == '-v': # prints verbose messages (only the success message, really)
            isVerbose = True

    # Set up GPUdb with binary encoding
    if GPUdb_URL:
        gpudb = GPUdb( host = GPUdb_URL, username = GPUdb_User, password = GPUdb_Pass, encoding='BINARY' )
    else:
        gpudb = GPUdb( encoding='BINARY', host = GPUdb_IP, port = GPUdb_Port )


    # Create a data type
    point_schema_str = """{
                              "type":"record",
                              "name":"point",
                              "fields":
                               [
                                 {"name":"x","type":"double"},
                                 {"name":"y","type":"double"},
                                 {"name":"OBJECT_ID","type":"string"}
                               ]
                            }""".replace(' ','').replace('\n','')

    # Register the data type and ensure that it worked
    # Endpoint: /create/type
    create_resp = gpudb.create_type ( point_schema_str, "point_type" )
    assert create_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to create point data type; error message: " \
                                              % create_resp['status_info'][ 'message' ]

    # Using the registered type's ID, create a new set (and check that worked)
    # Endpoint: /create/table
    type_id = create_resp[ 'type_id' ]
    # Get a table name without a period (not allowed in 7.1)
    table_name = get_prefix_table_name_with_current_datetime( "diagnostics_point_set" )
    create_table_resp = gpudb.create_table( table_name, type_id ) # not a part of a collection
    assert create_table_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to create point table; error message: %s" \
                                              % create_table_resp['status_info'][ 'message' ]

    # Add some data to the set in batches
    # Endpoint: /insert/records/random
    count_1 = 2000
    param_map_1 = { "x": {"min": 0, "max": 42 }, "y": {"min": 0, "max": 42 } }
    random_resp = gpudb.insert_records_random( table_name, count_1, param_map_1 )

    # Check that the first set of objects were generated successfully
    assert random_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to generate random points; error message: %s" \
                                              % random_resp['status_info'][ 'message' ]

    # Add another batch of data points to the same set, but at a different location
    # Endpoint: /insert/records/random
    count_2 = 2000
    param_map_2 = { "x": {"min": -50, "max": -20 }, "y": {"min": -50, "max": -20 } }
    random_resp = gpudb.insert_records_random( table_name, count_2, param_map_2 )

    # Check that the first set of objects were generated successfully
    assert random_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to generate random points; error message: %s" \
                                              % random_resp['status_info'][ 'message' ]

    # Check the total size of the set is as intended
    # Endpoint: /show/table
    total_size = count_1 + count_2
    show_table_resp = gpudb.show_table( table_name, options = {"get_sizes": "true"} )
    assert show_table_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to check status of set; error message: %s" \
                                              % show_table_resp['status_info'][ 'message' ]
    assert show_table_resp[ 'total_size' ] == total_size, "Error: Total size of set is not as expected. Set size = %s, expected size = %s" % ( show_table_resp[ 'total_size' ], total_size )

    # Query chaining: do two filters one after another, get final count
    # Do a similar query with select, check count against the chained queries

    # Bounding box: x within [10, 20] and y within [10, 20]
    # Endpoint: /filter/bybox
    bbox_view_name = get_prefix_table_name_with_current_datetime( "diagnostics_bbox_result" )
    bbox_resp = gpudb.filter_by_box( table_name, bbox_view_name, "x", 10, 20, "y", 10, 20 )
    assert bbox_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform bounding box query; error message: %s" \
                                              % bbox_resp['status_info'][ 'message' ]

    # Filter by radius: 100km radius around (lon, lat) = (15, 15)
    # Endpoint: /filter/byradius
    fradius_view_name = get_prefix_table_name_with_current_datetime( "diagnostics_filter_by_radius_result" )
    fradius_resp = gpudb.filter_by_radius( bbox_view_name, fradius_view_name, "x", 15, "y", 15, 100000 )
    assert fradius_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform filter by radius query; error message: %s" \
                                              % fradius_resp['status_info'][ 'message' ]

    # Do a select query with a predicate that should yield the same result
    # as the above chained queries
    # Select: ( (10 <= x) and (x <= 20) and (10 <= y) and (y <= 20) and (geodist(x, y, 15, 15) < 100000) )
    # Endpoint: /filter
    filter_view_name = get_prefix_table_name_with_current_datetime( "diagnostics_filter_result" )
    predicate = "( (10 <= x) and (x <= 20) and (10 <= y) and (y <= 20) and (geodist(x, y, 15, 15) < 100000) )"
    filter_resp = gpudb.filter( table_name, filter_view_name, predicate )
    assert filter_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform filter query; error message: %s" \
                                              % filter_resp['status_info'][ 'message' ]
    assert filter_resp[ 'count' ] == fradius_resp[ 'count' ], "Error: Mismatch in counts of filter (%s) and chained queries (bounding box then filter by radius) (%s)" \
                                              % ( filter_resp[ 'count' ], fradius_resp[ 'count' ] )


    # Delete a few objects and check the set size of the original set
    #
    # Delete objects: Delte a few objects given a predicate
    # Endpoint: /delete/records
    delete_expression = ["((15 <= x) and (x <= 18.5))"]
    delete_resp = gpudb.delete_records( table_name, delete_expression )
    assert delete_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform delete operation; error message: %s" \
                                              % delete_resp['status_info'][ 'message' ]

    # Check that the size of the set has gone down
    # Statistics return the count as a default
    # Endpoint: /aggregate/statistics
    new_size = total_size - delete_resp[ 'count_deleted' ]
    statistics_resp = gpudb.aggregate_statistics( table_name, "x", "count" )
    assert statistics_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform the statistics operation; error message: %s" \
                                              % statistics_resp['status_info'][ 'message' ]
    assert statistics_resp[ 'stats' ][ 'count' ] == new_size, "Error: Mismatch in counts of set size (%s) and expected size (%s)" \
                                              % ( statistics_resp[ 'count' ], new_size )

    # Update a few objects and check the update was successful by doing a select
    #
    # Update objects based on x, change the y value
    # Endpoing: /update/records
    update_predicate = "((-35 <= x) and (x <= -33.5))"
    update_resp = gpudb.update_records( table_name, [ update_predicate ], [{'y': "71"}] )
    assert update_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform the update operation; error message: %s" \
                                              % update_resp['status_info'][ 'message' ]

    # Check that the selected objects' y values have been changed
    #
    # Obtain the selected objects by performing a select query
    # Endpoint: /filter
    filter_view_name2 = get_prefix_table_name_with_current_datetime( "diagnostics_filter_result_2" )
    filter_resp1 = gpudb.filter( table_name, filter_view_name2, update_predicate )
    assert filter_resp1['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform filter operation; error message: %s" \
                                              % filter_resp1['status_info'][ 'message' ]

    # Get all the objects in the resultant set that has the update y value
    # and check that it matches with the above count
    # Endpont: /filter
    filter_expression = "(y == 71)"
    filter_view_name3 = get_prefix_table_name_with_current_datetime( "diagnostics_filter_result_3" )
    filter_resp2 = gpudb.filter( table_name, filter_view_name3, filter_expression )
    assert filter_resp2['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform filter operation; error message: %s" \
                                              % filter_resp2['status_info'][ 'message' ]
    # Now check that the counts match
    assert filter_resp1[ 'count' ] == filter_resp2[ 'count' ], "GPUdb failed in performing update correctly; expected count is %s, but given count is %s" \
                                                              % ( filter_resp1[ 'count' ], filter_resp2[ 'count' ] )

    # Clear all the tables (dropping the original table also drops views)
    clear_resp = gpudb.clear_table( table_name )
    assert clear_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed in clearing set %s" % table_name

    if isVerbose:
        print ( "The diagnostics tests succeeded!" )
# end diagnose_gpudb



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    diagnose_gpudb( sys.argv )
