import sys
# Add the path to the API file so that it can be imported
sys.path.insert(0, '../gpudb'  )

import cStringIO
import collections
from avro import schema, io


import gpudb



def gpudb_example():
    """An example of how to use the GPUdb python API.
    """

    new_line = '\n'

    # handle to database
    h_db = gpudb.GPUdb(encoding = 'BINARY', host = '127.0.0.1', port = '9191')

    my_table='my_table_1'

    # Data type for the table
    my_type = """
    {
        "type": "record",
        "name": "my_type_1",
        "fields": [
            {"name":"col1","type":"double"},
            {"name":"col2","type":"string"},
            {"name":"group_id","type":"string"}
        ]
    }  """.replace(' ','').replace('\n','')

    # Create the data type in the DB
    response = h_db.create_type( type_definition = my_type, label = 'my_type_lb_1' )
    type_id_1 = response['type_id']
    print 'GPUdb generated type id for the new type - ', type_id_1, new_line

    # Create a table with the given data type
    response = h_db.create_table( table_name = my_table, type_id = type_id_1 )

    # Generate data to be inserted into the table
    encoded_obj_list = []

    for val in range(1,10):
        datum = collections.OrderedDict()
        datum["col1"] = val+0.1
        datum["col2"] = 'string '+str(val)
        datum["group_id"] = 'Group 1'
        # Encode the data appropriately to prepare for insertion
        encoded_obj_list.append(h_db.encode_datum(my_type, datum))

    # Optional parameter that enables returning IDs for the
    # newly inserted records
    options = {'return_record_ids':'true'}

    # Insert the records into the table
    response = h_db.insert_records( table_name = my_table,
                                    objects = encoded_obj_list,
                                    list_encoding = 'binary',
                                    options = options )
    print "Record Ids for %d new records - %s" % (response['count_inserted'], response['record_ids']), new_line

    # Retrieve records from a table. Note that the records are stringified and have to be parsed
    response = h_db.get_records(table_name=my_table, offset=0, limit=100, encoding='json', options={})
    print "Returned records ", response['records_json'], new_line

    # Filter records into a view.  Response contains the count only
    response = h_db.filter(table_name=my_table, view_name='my_table_view', expression='col1 = 1.1')
    print "Number of records returned by filter expresion ", response['count'], new_line

    # Read the filtered records from the view (exactly as reading from table)
    response = h_db.get_records( table_name = 'my_table_view',
                                 offset = 0, limit = 100,
                                 encoding = 'json', options = {} )
    print "Filtered records ", response['records_json'], new_line

    # Drop the view
    h_db.clear_table('my_table_view')

    # Filter expression with two columns on the original table
    response = h_db.filter(my_table,'my_table_view','col1 <= 9 and group_id="Group 1"')
    print "Number of records returned by second filter expresion ", response['count'], new_line

    # Fetch the records from the view
    response = h_db.get_records( table_name = 'my_table_view',
                                 offset = 0, limit = 100, encoding = 'json',
				 options = {} )
    print "Returned records ", response['records_json'], new_line

    # Filter by a list.  query is executed on resultset from previous query (query chaining)
    response = h_db.filter_by_list( table_name = 'my_table_view',
                                    view_name = 'my_table_view_2',
                                    column_values_map = {'col1': ['1.1', '2.1', '5.1' ] } )
    print "Number of records returned by filter expresion ", response['count'], new_line

    # Fetch the records
    response = h_db.get_records( table_name = 'my_table_view_2',
                                 offset = 0, limit = 100,
                                 encoding = 'json', options = {} )
    print "Returned records filtered by list: ", response['records_json'], new_line

    # filter a range of values (numeric values only)
    response = h_db.filter_by_range( table_name = my_table,
                                     view_name = 'my_table_view_3',
				     column_name = 'col1',
				     lower_bound = 1,
				     upper_bound = 5 )
    print "Number of records returned by filter expresion ", response['count'], new_line

    response = h_db.get_records(table_name='my_table_view_3', offset=0, limit=100, encoding='binary', options={})

    # Decoding the binary encoded response
    print "Returned records filtered by range: "
    parsed_schema = schema.parse( response['type_schema'] )
    reader = io.DatumReader( parsed_schema )
    for bin_record in response['records_binary']:
        str_IO = cStringIO.StringIO( bin_record )
        bin_decoder = io.BinaryDecoder( str_IO )
        decoded_response = reader.read( bin_decoder )
        print decoded_response, new_line




    response = h_db.aggregate_statistics(table_name=my_table, column_name='col1', stats='count,sum,mean')
    print "Statistics of values in col1 ", response['stats'], new_line

    encoded_obj_list=[]
    for val in range(1,8):
        datum = collections.OrderedDict()
        datum["col1"] = val+10.1
        datum["col2"] = 'string '+str(val)
        datum["group_id"] = 'Group 2'
        encoded_obj_list.append(h_db.encode_datum(my_type, datum))

    h_db.insert_records(my_table,encoded_obj_list,'binary',{})

    # find unique values in a column
    response = h_db.aggregate_unique( table_name = my_table,
                                      column_name = 'group_id',
                                      offset = 0, limit = 20,
                                      encoding = 'json')
    print 'Unique values in group_id column ', response['json_encoded_response'], new_line

    # Group by
    groupby_col_names = ['col2']
    retval = h_db.aggregate_group_by(table_name=my_table, column_names=groupby_col_names, offset=0, limit=1000,encoding='json')
    print "Group by results ", retval['json_encoded_response'], new_line

    # aggregate values
    groupby_col_names = ['group_id', "count(*)", 'sum(col1)', 'avg(col1)']
    retval = h_db.aggregate_group_by(table_name=my_table, column_names=groupby_col_names, offset=0, limit=1000,encoding='json')
    print "Group by results ", retval['json_encoded_response'], new_line

    # Do another aggregate group by operation
    groupby_col_names = ['group_id', 'sum(col1*10)']
    retval = h_db.aggregate_group_by( table_name = my_table,
                                      column_names = groupby_col_names,
				      offset = 0, limit = 1000,
				      encoding = 'json')
    print "Group by results ", retval['json_encoded_response'], new_line

    # Add more data
    for val in range(4,10):
        datum = collections.OrderedDict()
        datum["col1"] = val+0.6
        datum["col2"] = 'string 2'+str(val)
        datum["group_id"] = 'Group 1'
        encoded_obj_list.append(h_db.encode_datum(my_type, datum))
    h_db.insert_records( table_name = my_table,
                         objects = encoded_obj_list,
                         list_encoding = 'binary',
                         options = {} )

    histogram_result = h_db.aggregate_histogram( table_name = my_table,
                                                 column_name = 'col1',
                                                 start = 1.1, end = 2,
                                                 interval = 1 )
    print "histogram result:", histogram_result, new_line



    # Drop the table (will automatically drop all views on the table)
    h_db.clear_table(my_table)

    # Check that clearing a table automatically drops all the dependent views
    response = h_db.get_records( table_name = 'my_table_view',
                                 offset = 0, limit = 100,
                                 encoding = 'json', options = {})
    assert (response['status_info']['status'] == "ERROR"), \
        "Problem: View on deleted table found!!!"
    print ("Response status and message : ", response['status_info']['status'], 
           ' - ', response['status_info']['message'], new_line)

# end example()





if __name__ == '__main__':
    gpudb_example()






