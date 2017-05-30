import sys

import cStringIO
import collections
from avro import schema, io


import gpudb



def gpudb_example():
    """An example of how to use the GPUdb python API.
    """

    new_line = '\n'

    # handle to database, one host
    h_db = gpudb.GPUdb(encoding = 'BINARY', host = '127.0.0.1', port = '9191')

    # handle to database, multiple hosts
    h_ha_db = gpudb.GPUdb(host=['localhost', 'localhost'],
                          port=['9191', '9192'])

    # All table and view names used in this example
    my_table = 'my_table_1'
    view_name_1 = 'my_table_view'
    view_name_2 = 'my_table_view_2'
    view_name_3 = 'my_table_view_3'
    # Clear all the above tables/views in case they already exist
    names = [ my_table, view_name_1, view_name_2, view_name_3 ]
    for name in names:
        h_db.clear_table( name )

    # Create a data type for the table
    # --------------------------------
    # First create the columns (column args: first the name, then the type, and then properties, if any)
    columns = []
    columns.append( gpudb.GPUdbRecordColumn( "col1", gpudb.GPUdbRecordColumn._ColumnType.DOUBLE ) )
    columns.append( gpudb.GPUdbRecordColumn( "col2", gpudb.GPUdbRecordColumn._ColumnType.STRING,
                                             [ gpudb.GPUdbColumnProperty.NULLABLE ] ) )
    columns.append( gpudb.GPUdbRecordColumn( "group_id", gpudb.GPUdbRecordColumn._ColumnType.STRING ) )

    # Create the type object
    record_type = gpudb.GPUdbRecordType( columns, label = 'my_type_lb_1' )

    # Create the type in the database (save the type ID; we'll need it to create the table)
    record_type.create_type( h_db )
    type_id_1 = record_type.type_id
    print 'GPUdb generated type id for the new type - ', type_id_1, new_line

    # Create a table with the given data type
    response = h_db.create_table( table_name = my_table, type_id = type_id_1 )

    # Generate data to be inserted into the table
    encoded_obj_list = []
    for val in range(1,10):
        col1_val     = ( val + 0.1 )
        # Using None for one of the records; this sets the value to null
        col2_val     = ('string '+str(val)) if (val != 5) else None
        group_id_val = 'Group 1'

        # Create a record: need the record type, and since we're using the list
        # constructor, we need to provide the column values in declaration order
        record = gpudb.GPUdbRecord( record_type, [ col1_val, col2_val, group_id_val ] )

        # Save the binary encoded record
        encoded_obj_list.append( record.binary_data )
    # end for loop


    # Optional parameter that enables returning IDs for the
    # newly inserted records
    options = {'return_record_ids':'true'}

    # Insert the records into the table
    response = h_db.insert_records( table_name = my_table,
                                    data = encoded_obj_list,
                                    list_encoding = 'binary',
                                    options = options )
    print "Record Ids for %d new records - %s" % (response['count_inserted'], response['record_ids']), new_line

    # Retrieve records from a table. Note that the records are stringified and have to be parsed
    response = h_db.get_records( table_name = my_table, offset = 0, limit = 100, encoding = 'json' )
    print "Returned records ", response['records_json'], new_line

    # Filter records into a view.  Response contains the count only
    response = h_db.filter(table_name = my_table, view_name = view_name_1, expression = 'col1 = 1.1')
    print "Number of records returned by filter expresion ", response['count'], new_line

    # Read the filtered records from the view (exactly as reading from table)
    response = h_db.get_records( table_name = 'my_table_view',
                                 offset = 0, limit = 100,
                                 encoding = 'json' )
    # Decode the JSON encoded data
    decoded_data = gpudb.GPUdbRecord.decode_json_string_data( response['records_json'] )
    print "Filtered records: "
    print "\n".join( [ str( d ) for d in decoded_data ] ), new_line

    # Drop the view
    h_db.clear_table('my_table_view')

    # Filter expression with two columns on the original table
    response = h_db.filter( my_table, 'my_table_view', 'col1 <= 9 and group_id="Group 1"' )
    print "Number of records returned by second filter expresion ", response['count'], new_line

    # Fetch the records from the view
    response = h_db.get_records( table_name = view_name_1,
                                 offset = 0, limit = 100, encoding = 'json' )
    print "Returned records ", response['records_json'], new_line

    # Filter by a list.  query is executed on resultset from previous query (query chaining)
    response = h_db.filter_by_list( table_name = view_name_1,
                                    view_name  = view_name_2,
                                    column_values_map = {'col1': ['1.1', '2.1', '5.1' ] } )
    print "Number of records returned by filter expresion ", response['count'], new_line

    # Fetch the records
    response = h_db.get_records( table_name = view_name_2,
                                 offset = 0, limit = 100,
                                 encoding = 'json' )
    print "Returned records filtered by list: ", response['records_json'], new_line

    # filter a range of values (numeric values only)
    response = h_db.filter_by_range( table_name = my_table,
                                     view_name  = view_name_3,
				     column_name = 'col1',
				     lower_bound = 1,
				     upper_bound = 5 )
    print "Number of records returned by filter expresion ", response['count'], new_line

    # Get the records, using the binary encoding this time
    response = h_db.get_records( table_name = view_name_3, offset = 0, limit = 100, encoding = 'binary' )

    # Decode the binary encoded response
    print "Returned records filtered by range: "
    decoded_data = gpudb.GPUdbRecord.decode_binary_data( response["type_schema"], response["records_binary"] )
    print "\n".join( [ str( d ) for d in decoded_data ] ), new_line


    # Aggregate some statistics on the data
    response = h_db.aggregate_statistics( table_name = my_table, column_name = 'col1', stats = 'count,sum,mean' )
    print "Statistics of values in col1 ", response['stats'], new_line

    # Add some more data
    encoded_obj_list=[]
    for val in range(1,8):
        # Create a record: need the record type, and since we're using the list
        # constructor, we need to provide the column values in declaration order
        record = gpudb.GPUdbRecord( record_type, [ (val+10.1), ('string '+str(val)), 'Group 2' ] )
        # Save the binary encoded record
        encoded_obj_list.append( record.binary_data )
    # end for loop

    # Insert the data (the default encoding is binary)
    h_db.insert_records( my_table, encoded_obj_list )

    # Find unique values in a column
    response = h_db.aggregate_unique( table_name = my_table,
                                      column_name = 'group_id',
                                      offset = 0, limit = 20,
                                      encoding = 'json' )
    # Parse the special dynamically created table's data
    print 'Unique values in group_id column:', new_line
    h_db.parse_dynamic_response( response, do_print = True )
    print new_line

    # Group by
    groupby_col_names = ['col2']
    retval = h_db.aggregate_group_by( table_name = my_table,
                                      column_names = groupby_col_names,
                                      offset = 0, limit = 1000,
                                      encoding = 'json' )
    print "Group by results ", retval['json_encoded_response'], new_line

    # aggregate values
    groupby_col_names = ['group_id', "count(*)", 'sum(col1)', 'avg(col1)']
    retval = h_db.aggregate_group_by( table_name = my_table,
                                      column_names = groupby_col_names,
                                      offset = 0, limit = 1000,
                                      encoding = 'json' )
    print "Group by results ", retval['json_encoded_response'], new_line

    # Do another aggregate group by operation
    groupby_col_names = ['group_id', 'sum(col1*10)']
    retval = h_db.aggregate_group_by( table_name = my_table,
                                      column_names = groupby_col_names,
				      offset = 0, limit = 1000,
				      encoding = 'json' )
    print "Group by results ", retval['json_encoded_response'], new_line

    # Generate more data
    for val in range(4,10):
        # Create a record: need the record type, and since we're using the list
        # constructor, we need to provide the column values in declaration order
        record = gpudb.GPUdbRecord( record_type, [ (val+0.6), ('string 2'+str(val)), 'Group 1' ] )
        # Save the binary encoded record
        encoded_obj_list.append( record.binary_data )
    # end for loop

    # Insert the data
    h_db.insert_records( table_name = my_table, data = encoded_obj_list )

    # Do a histogram on the data
    histogram_result = h_db.aggregate_histogram( table_name = my_table,
                                                 column_name = 'col1',
                                                 start = 1.1, end = 2,
                                                 interval = 1 )
    print "Histogram result:", histogram_result, new_line



    # Drop the table (will automatically drop all views on the table)
    h_db.clear_table(my_table)

    # Check that clearing a table automatically drops all the dependent views
    response = h_db.get_records( table_name = 'my_table_view',
                                 offset = 0, limit = 100,
                                 encoding = 'json' )
    assert (response['status_info']['status'] == "ERROR"), \
        "Problem: View on deleted table found!!!"

# end example()





if __name__ == '__main__':
    gpudb_example()






