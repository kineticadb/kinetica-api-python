#!/usr/bin/python

# ######################################################
# 
# Script to run a diagnostic test on GPUdb
#
# @file gpudb_diagnostics.py
# @author Meem Mahmud
# ######################################################



from gpudb import GPUdb
import datetime
import sys
import getopt




helpMessage = """Usage:
  -l -- Run script on local GPUdb (127.0.0.1:9191) (lower case L)
        (This is default if neither -l nor -g is specified)
  -g ###.##.##.## -- Run script on server GPUdb @ the IP address provided
  -p #### -- Run script on  the specified port (default is 9191)
  -h -- Print this help message
"""


def diagnose_gpudb( argv ):
    """
    Run a diagnostic test on GPUdb
    Argument:
      argv -- Command line arguments
    """

    # Parse the command line arguments
    if ( len(sys.argv) == 1 ): # None provided
        # Print help message and quit
        print helpMessage
        sys.exit( 2 )
    try: # Parse the command line arguments
        opts, args = getopt.getopt( sys.argv[1:], "hlg:p:" )
    except getopt.GetoptError:
        print helpMessage
        sys.exit( 2 )

    # Some default values
    GPUdb_IP = '127.0.0.1' # Run locally by default
    GPUdb_Port = '9191' # Default port

    # Parse the arguments
    for opt, arg in opts:
        if opt == '-h': # print usage and exit
            print helpMessage
            sys.exit()
        if opt == '-l': # run gpudb on local machine
            isServer = False
        if opt == '-g': # run gpudb on a server gpudb at the specified IP address
            GPUdb_IP = arg
            set_id = "TwitterPointText" # Default set ID for server gpudb
        if opt == '-p': # run gpudb on a server gpudb at the specified port
            GPUdb_Port = arg

    # Set up GPUdb with binary encoding
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
    # Endpoint: /registertype
    register_resp = gpudb.register_type ( point_schema_str, "", "point_type", "POINT" )
    assert register_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to register point data type; error message: " \
                                              % register_resp['status_info'][ 'message' ]

    # Using the registered type's ID, create a new set (and check that worked)
    # Endpoint: /newset
    type_id = register_resp[ 'type_id' ]
    set_id = "diagnostics_point_set_" + datetime.datetime.now().isoformat()
    new_set_resp = gpudb.new_set( type_id, set_id, "" ) # no parent set ID
    assert new_set_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to create point set; error message: %s" \
                                              % new_set_resp['status_info'][ 'message' ]

    # Add some data to the set in batches
    # Endpoint: /random
    count_1 = 2000
    param_map_1 = { "x": {"min": 0, "max": 42 }, "y": {"min": 0, "max": 42 } }
    random_resp = gpudb.random( set_id, count_1, param_map_1 )

    # Check that the first set of objects were generated successfully
    assert random_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to generate random points; error message: %s" \
                                              % random_resp['status_info'][ 'message' ]

    # Add another batch of data points to the same set, but at a different location
    # Endpoint: /random
    count_2 = 2000
    param_map_2 = { "x": {"min": -50, "max": -20 }, "y": {"min": -50, "max": -20 } }
    random_resp = gpudb.random( set_id, count_2, param_map_2 )

    # Check that the first set of objects were generated successfully
    assert random_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to generate random points; error message: %s" \
                                              % random_resp['status_info'][ 'message' ]

    # Check the total size of the set is as intended
    # Endpoint: /status
    total_size = count_1 + count_2
    status_resp = gpudb.status( set_id )
    assert status_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to check status of set; error message: %s" \
                                              % status_resp['status_info'][ 'message' ]
    assert status_resp[ 'total_size' ] == total_size, "Error: Total size of set is not as expected. Set size = %s, expected size = %s" % ( status_resp[ 'total_size' ], total_size )

    # Query chaining: do two filters one after another, get final count
    # Do a similar query with select, check count against the chained queries

    # Bounding box: x within [10, 20] and y within [10, 20]
    # Endpoint: /boundingbox
    bbox_set_id = "diagnostics_bbox_result_" + datetime.datetime.now().isoformat()
    bbox_resp = gpudb.bounding_box( 10, 20, 10, 20, "x", "y", set_id, bbox_set_id )
    assert bbox_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform bounding box query; error message: %s" \
                                              % bbox_resp['status_info'][ 'message' ]

    # Filter by radius: 100km radius around (lon, lat) = (15, 15)
    # Endpoint: /filterbyradius
    fradius_set_id = "diagnostics_filter_by_radius_result_" + datetime.datetime.now().isoformat()
    fradius_resp = gpudb.filter_by_radius( bbox_set_id, "x", "y", 15, 15, 100000, fradius_set_id )
    assert fradius_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform filter by radius query; error message: %s" \
                                              % fradius_resp['status_info'][ 'message' ]

    # Do a select query with a predicate that should yield the same result
    # as the above chained queries
    # Select: ( (10 <= x) and (x <= 20) and (10 <= y) and (y <= 20) and (geodist(x, y, 15, 15) < 100000) )
    # Endpoint: /select
    select_set_id = "diagnostics_select_result_" + datetime.datetime.now().isoformat()
    predicate = "( (10 <= x) and (x <= 20) and (10 <= y) and (y <= 20) and (geodist(x, y, 15, 15) < 100000) )"
    select_resp = gpudb.select( set_id, select_set_id, predicate )
    assert select_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform select query; error message: %s" \
                                              % select_resp['status_info'][ 'message' ]
    assert select_resp[ 'count' ] == fradius_resp[ 'count' ], "Error: Mismatch in counts of select (%s) and chained queries (bounding box then filter by radius) (%s)" \
                                              % ( select_resp[ 'count' ], fradius_resp[ 'count' ] )


    # Delete a few objects and check the set size of the original set
    #
    # Delete objects: Delte a few objects given a predicate
    # Endpoint: /selectdelete
    delete_predicate = "((15 <= x) and (x <= 18.5))"
    delete_resp = gpudb.select_delete( set_id, delete_predicate )
    assert delete_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform select delete operation; error message: %s" \
                                              % delete_resp['status_info'][ 'message' ]

    # Check that the size of the set has gone down
    # Statistics return the count as a default
    # Endpoint: /statistics
    new_size = total_size - delete_resp[ 'count' ]
    statistics_resp = gpudb.statistics( set_id, "x", "sum" )
    assert statistics_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform statistics operation; error message: %s" \
                                              % statistics_resp['status_info'][ 'message' ]
    assert statistics_resp[ 'stats' ][ 'count' ] == new_size, "Error: Mismatch in counts of set size (%s) and expected size (%s)" \
                                              % ( statistics_resp[ 'count' ], new_size )

    # Update a few objects and check the update was successful by doing a select
    #
    # Update objects based on x, change the y value
    # Endpoing: /selectupdate
    update_predicate = "((-35 <= x) and (x <= -33.5))"
    update_resp = gpudb.select_update( set_id, update_predicate, {'y': "71"} )
    assert update_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform select update operation; error message: %s" \
                                              % update_resp['status_info'][ 'message' ]

    # Check that the selected objects' y values have been changed
    #
    # Obtain the selected objects by performing a select query
    # Endpoint: /select
    select_set_id2 = "diagnostics_select_result_2_" + datetime.datetime.now().isoformat()
    select_resp1 = gpudb.select( set_id, select_set_id2, update_predicate )
    assert select_resp1['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform select operation; error message: %s" \
                                              % select_resp1['status_info'][ 'message' ]

    # Get all the objects in the resultant set that has the update y value
    # and check that it matches with the above count
    # Endpont: /select
    select_predicate = "(y == 71)"
    select_set_id3 = "diagnostics_select_result_3_" + datetime.datetime.now().isoformat()
    select_resp2 = gpudb.select( set_id, select_set_id3, select_predicate )
    assert select_resp2['status_info'][ 'status' ] == 'OK', "GPUdb failed to perform select operation; error message: %s" \
                                              % select_resp2['status_info'][ 'message' ]
    # Now check that the counts match
    assert select_resp1[ 'count' ] == select_resp2[ 'count' ], "GPUdb failed in performing select update correctly; expected count is %s, but given count is %s" \
                                                              % ( select_resp1[ 'count' ], select_resp2[ 'count' ] )

    # Clear all the sets
    clear_resp = gpudb.clear( set_id )
    assert clear_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed in clearing set %s" % set_id
    clear_resp = gpudb.clear( bbox_set_id )
    assert clear_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed in clearing set %s" % bbox_set_id
    clear_resp = gpudb.clear( fradius_set_id )
    assert clear_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed in clearing set %s" % fradius_set_id
    clear_resp = gpudb.clear( select_set_id )
    assert clear_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed in clearing set %s" % select_set_id
    clear_resp = gpudb.clear( select_set_id2 )
    assert clear_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed in clearing set %s" % select_set_id2
    clear_resp = gpudb.clear( select_set_id3 )
    assert clear_resp['status_info'][ 'status' ] == 'OK', "GPUdb failed in clearing set %s" % select_set_id3

# end diagnose_gpudb



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    diagnose_gpudb( sys.argv )
