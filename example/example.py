"""This script walks through how to use the Python API.

Covered here: importing GPUdb, instantiating Kinetica, creating a type,
creating a table, inserting records, retrieving records, filtering records,
aggregating/grouping records, and deleting records.
"""

from __future__ import print_function

import collections
import json
import random
import string

import gpudb


def gpudb_example():
    
    print ( "TUTORIAL OUTPUT")
    print ( "===============\n")

    # all tables/views used in examples below
    weather_table_name = "weather"
    weather_w_view = "weather_west"
    weather_nw_view = "weather_northwest"
    weather_country_view = "weather_country"
    weather_e_view = "weather_east"
    weather_se_view = "weather_southeast"
    weather_h_view = "weather_histogram"

    """ Establish connection with a locally-running instance of Kinetica,
        using binary encoding to save memory """
    h_db = gpudb.GPUdb(encoding='BINARY', host='127.0.0.1', port='9191')

    print ()
    print ( "CREATING A TYPE & TABLE")
    print ( "-----------------------")
    print ()

    """ Create columns; column arguments consist of a list of the name, then type, and then
        optional additional properties.  E.g., [ "column_name", column_type, column_property1,
        column_property2 ].  Note that any number of column properties can be listed as long as
        they are not mutually exclusive within themselves or with the primitive type.  Also note
        that raw string can be used for both the primitive type and the properties; but the user is
        also able to use string constants as illustrated in the example below.
    """
    columns = [
        [ "city", "string", "char16" ],
        [ "state_province", gpudb.GPUdbRecordColumn._ColumnType.STRING, gpudb.GPUdbColumnProperty.CHAR32 ],
        [ "country", gpudb.GPUdbRecordColumn._ColumnType.STRING, gpudb.GPUdbColumnProperty.CHAR16 ],
        [ "x", "double" ],
        [ "y", "double" ],
        [ "avg_temp", "double" ],
        [ "time_zone", "string", "char8" ]
    ]

    # Clear any existing table with the same name (otherwise we won't be able to
    # create the table)
    if h_db.has_table( table_name = weather_table_name )['table_exists']:
        h_db.clear_table( weather_table_name )

    # Create the table from the type
    try:
        weather_table = gpudb.GPUdbTable( columns, weather_table_name, db = h_db )
        print ( "Table successfully created.")
    except gpudb.GPUdbException as e:
        print ( "Table creation failure: {}".format( str(e) ) )


    # We can also create a GPUdbTable object for a table that already exists in
    # the database.  All we need is the table name (and a GPUdb object).  Note how
    # we pass None for the type argument
    weather_table_duplicate = gpudb.GPUdbTable( None, weather_table_name, db = h_db )

    print ( "\n")
    print ( "INSERTING DATA")
    print ( "--------------")
    print ()

    # Insert single record example

    # Create ordered dictionary for keys & values of record
    datum = collections.OrderedDict()
    datum["city"] = "Washington, D.C."
    datum["state_province"] = "--"
    datum["country"] = "USA"
    datum["x"] = -77.016389
    datum["y"] = 38.904722
    datum["avg_temp"] = 58.5
    datum["time_zone"] = "UTC-5"

    # Insert the record into the table (through the GPUdbTable interface)
    weather_table.insert_records( datum )

    # Create another record
    datum2 = collections.OrderedDict()
    datum2["city"] = "Washington, D.C."
    datum2["state_province"] = "--"
    datum2["country"] = "USA"
    datum2["x"] = -77.016389
    datum2["y"] = 38.904722
    datum2["avg_temp"] = 58.5
    datum2["time_zone"] = "UTC-5"

    # Insert the second record through the basic GPUdb interface
    # Encode record and put into a single element list
    weather_record_type = weather_table.get_table_type()
    single_record = [ gpudb.GPUdbRecord( weather_record_type, datum ).binary_data ]

    # Insert the record into the table
    response = h_db.insert_records(table_name = weather_table_name, data = single_record, list_encoding = "binary")
    print ( "Number of single records inserted:  {}".format(response["count_inserted"]))


    # Insert multiple records example
    # ===============================
    records = []
    # Create a list of in-line records
    records.append( ["Paris", "TX", "USA", -95.547778, 33.6625, 64.6, "UTC-6"] )
    records.append( ["Memphis", "TN", "USA", -89.971111, 35.1175, 63, "UTC-6"] )
    records.append( ["Sydney", "Nova Scotia", "Canada", -60.19551, 46.13631, 44.5, "UTC-4"] )
    records.append( ["La Paz", "Baja California Sur", "Mexico", -110.310833, 24.142222, 77, "UTC-7"] )
    records.append( ["St. Petersburg", "FL", "USA", -82.64, 27.773056, 74.5, "UTC-5"] )
    records.append( ["Oslo", "--", "Norway", 10.75, 59.95, 45.5, "UTC+1"] )
    records.append( ["Paris", "--", "France", 2.3508, 48.8567, 56.5, "UTC+1"] )
    records.append( ["Memphis", "--", "Egypt", 31.250833, 29.844722, 73, "UTC+2"] )
    records.append( ["St. Petersburg", "--", "Russia", 30.3, 59.95, 43.5, "UTC+3"] )
    records.append( ["Lagos", "Lagos", "Nigeria", 3.384082, 6.455027, 83, "UTC+1"] )
    records.append( ["La Paz", "Pedro Domingo Murillo", "Bolivia", -68.15, -16.5, 44, "UTC-4"] )
    records.append( ["Sao Paulo", "Sao Paulo", "Brazil", -46.633333, -23.55, 69.5, "UTC-3"] )
    records.append( ["Santiago", "Santiago Province", "Chile", -70.666667, -33.45, 62, "UTC-4"] )
    records.append( ["Buenos Aires", "--", "Argentina", -58.381667, -34.603333, 65, "UTC-3"] )
    records.append( ["Manaus", "Amazonas", "Brazil", -60.016667, -3.1, 83.5, "UTC-4"] )
    records.append( ["Sydney", "New South Wales", "Australia", 151.209444, -33.865, 63.5, "UTC+10"] )
    records.append( ["Auckland", "--", "New Zealand", 174.74, -36.840556, 60.5, "UTC+12"] )
    records.append( ["Jakarta", "--", "Indonesia", 106.816667, -6.2, 83, "UTC+7"] )
    records.append( ["Hobart", "--", "Tasmania", 147.325, -42.880556, 56, "UTC+10"] )
    records.append( ["Perth", "Western Australia", "Australia", 115.858889, -31.952222, 68, "UTC+8"] )

    # Insert the records into the table
    weather_table.insert_records( records )
    print ( "Number of batch records inserted:  {}".format( weather_table.size() ))

    print ( "\n")
    print ( "RETRIEVING DATA")
    print ( "---------------")
    print ()

    """ Retrieve the second set of ten records from weather_table. Note that
        records can be iterated over directly. """
    print ( "{:<20s} {:<25s} {:<15s} {:<10s} {:<11s} {:<9s} {:<8s}".format("City","State/Province","Country","Latitude","Longitude","Avg. Temp","Time Zone"))
    print ( "{:=<20s} {:=<25s} {:=<15s} {:=<10s} {:=<11s} {:=<9s} {:=<9s}".format("", "", "", "", "", "", ""))
    for weatherLoc in weather_table.get_records( offset = 10, limit = 10 ):
        print ( "{city:<20s} {state:<25s} {country:<15s} {y:10.6f} {x:11.6f} {avg_temp:9.1f}   {time_zone}"
                "".format( city = weatherLoc["city"], state = weatherLoc["state_province"], country = weatherLoc["country"],
                           y = weatherLoc["y"], x = weatherLoc["x"], avg_temp = weatherLoc["avg_temp"], time_zone = weatherLoc["time_zone"] ) )

    
    """ Retrieve no more than 10 records as JSON from weather_table through the GPUdb interface.
        Note that records are stringified and have to be parsed if using the 'json' encoding. """
    weatherLocs = h_db.get_records( table_name = weather_table_name, offset = 0, limit = 10,
                                    encoding = "json", options = {"sort_by":"city"} )['records_json']

    print ( "{:<20s} {:<25s} {:<15s} {:<10s} {:<11s} {:<9s} {:<8s}".format("City","State/Province","Country","Latitude","Longitude","Avg. Temp","Time Zone"))
    print ( "{:=<20s} {:=<25s} {:=<15s} {:=<10s} {:=<11s} {:=<9s} {:=<9s}".format("", "", "", "", "", "", ""))
    for weatherLoc in weatherLocs:
        print ( "{city:<20s} {state_province:<25s} {country:<15s} {y:10.6f} {x:11.6f} {avg_temp:9.1f}   {time_zone}".format(**json.loads(weatherLoc)))

    """ Retrieve no more than 25 of the remaining records as binary from weather
        table. Note that records are binary and have to be decoded. """
    response = h_db.get_records( table_name = weather_table_name, offset = 10, limit = 25,
                                 encoding = "binary", options = {"sort_by":"city"})
    weatherLocs = gpudb.GPUdbRecord.decode_binary_data(response["type_schema"], response["records_binary"])

    for weatherLoc in weatherLocs:
        print ( "{city:<20s} {state_province:<25s} {country:<15s} {y:10.6f} {x:11.6f} {avg_temp:9.1f}   {time_zone}".format(**weatherLoc))

    """ Note that total_number_of_records does not reflect offset/limit; it's
        the count of all records or those which match the given expression """
    print ( "\nNumber of records in new table:  {:d}".format(response["total_number_of_records"]))

    print ( "\n")
    print ( "FILTERING")
    print ( "---------")
    print ()

    ### Filter Example 1
    
    """ Filter records where column x is less than 0, i.e., cities in the
        western hemisphere, and store the filter in a view.  Note that the GPUdbTable
        creates a random view name if one is not supplied. """
    view1 = weather_table.filter( expression = "x < 0" )
    print ( "Number of records in the western hemisphere:  {}".format( view1.size() ))

    ### Filter Example 2
    
    """ Filter records where column x is less than 0 and column y is greater
        than 0, i.e., cities in the northwestern semi-hemisphere, and store
        the filter in a view.  This filter operation is done through the base
        GPUdb interface. """
    response = h_db.filter(table_name = weather_table_name, view_name = weather_nw_view,
                           expression = "x < 0 and y > 0" )
    print ( "Number of records in the northwestern semi-hemisphere:  {}".format( response["count"] ))

    ### Filter Example 3
    
    """ Filter records using the same expressions as Example 2, but using
        query chaining this time (note that we're using the view created by the
        first filter. """

    nw_view = view1.filter( expression = "y > 0" )
    print ( "Number of records in the northwestern semi-hemisphere (with query chaining):  {}"
            "".format( nw_view.size() ))

    ### Filter Example 4
    
    """ Filter by list where country name is USA, Brazil, or Australia.  Here we
        use the duplicate GPUdbTable object (but it points to the same DB table). """
    country_map = {"country": ["USA", "Brazil", "Australia"]}
    view3 = weather_table_duplicate.filter_by_list( column_values_map = country_map )
    print ( "Number of records where country name is USA, Brazil, or Australia:  {}"
            "".format( view3.size() ))

    ### Filter Example 5
    
    """ Filter by range cities that are east of GMT (the Prime Meridian) """
    view4 = weather_table.filter_by_range( column_name = "x", lower_bound = 0,
                                           upper_bound = 180 )
    print ( "Number of records that are east of the Prime Meridian (x > 0):  {}"
            "".format( view4.size() ))


    print ( "\n")
    print ( "AGGREGATING, GROUPING, and HISTOGRAMS")
    print ( "-------------------------------------")
    print ()

    ### Aggregate Example 1
    
    """ Aggregate count, min, mean, and max on the average temperature.  Note
        that unlike the filter functions, the aggregate functions of GPUdbTable
        return the response from the database. """
    stat_results = weather_table.aggregate_statistics( column_name = "avg_temp",
                                                       stats = "count,min,max,mean" )
    print ( "Statistics of values in the average temperature column:")
    print ( "\tCount: {count:.0f}\n\tMin:  {min:4.2f}\n\tMean: {mean:4.2f}\n\tMax:  {max:4.2f}"
            "\n".format( **stat_results["stats"] ))

    ### Aggregate Example 2
    
    """ Find unique city names. """
    results = weather_table.aggregate_unique( column_name = "city", offset = 0,
                                              limit = 25 )
    print ( "Unique city names:")
    for weatherLoc in results.data["city"]:
        print ( "\t* {}".format( weatherLoc ))
    print ()

    """ Same operation, but through the base GPUdb interface.  Note that the
        results have to parsed specially using GPUdb.parse_dynamic_response().
        Also, we're using the 'json' encoding in this case (the 'binary' encoding
        can also be used).  Also note how the data is accessed differently. """
    response = h_db.aggregate_unique( table_name = weather_table_name,
                                      column_name = "city", offset = 0,
                                      limit = 25, encoding = "json")
    print ( "Unique city names (using the GPUdb class):")
    weatherLocs = h_db.parse_dynamic_response(response)['response']['city']
    for weatherLoc in weatherLocs:
        print ( "\t* {}".format(weatherLoc))
    print ()

    ### Aggregate Example 3
    
    """ Find number of weather locations per country in the northwestern
        semi-hemisphere.  Note that the data is automatically decoded. """
    results = nw_view.aggregate_group_by( column_names = ["country", "count(country)"], offset = 0,
                                          limit = 25 )
    print ( "Weather locations per country in the northwest semi-hemisphere:")
    for country in zip(results.data["country"], results.data["count(country)"]):
        print ( "\t{:<10s}{:2d}".format(country[0] + ":", country[1]))
    print ()

    """ Find number of weather locations per country in the northwestern
        semi-hemisphere; use binary decoding explicitly since we're using
        the GPUdb class. """
    response = h_db.aggregate_group_by(table_name=weather_nw_view, column_names=["country", "count(country)"], offset=0, limit=25, encoding="binary")
    countries = gpudb.GPUdbRecord.decode_binary_data(response["response_schema_str"], response["binary_encoded_response"])
    print ( "Weather locations per country in the northwest semi-hemisphere:")
    for country in zip(countries["column_1"], countries["column_2"]):
        print ( "\t{:<10s}{:2d}".format(country[0] + ":", country[1]))
    print ()

    ### Aggregate Example 4
    
    """ Filter table to southeastern semi-hemisphere records, group by country,
        and aggregate min, max, and mean on the average temperature; using the default
        binary decoding and the GPUdbTable interface. """
    # Do a filter first
    se_view = weather_table.filter( expression="x > 0 and y < 0" )
    # Then do the aggregation operation (note how we use the 'data' property to get
    # the data)
    data = se_view.aggregate_group_by( column_names = ["country", "min(avg_temp)", "max(avg_temp)", "mean(avg_temp)"],
                                       offset = 0, limit = 25 ).data
    print ( "{:<20s} {:^5s} {:^5s} {:^5s}".format("SE Semi-Hemi Country", "Min", "Mean", "Max"))
    print ( "{:=<20s} {:=<5s} {:=<5s} {:=<5s}".format("", "", "", ""))
    for countryWeather in zip(data["country"], data["min(avg_temp)"], data["mean(avg_temp)"], data["max(avg_temp)"]):
        print ( "{:<20s} {:5.2f} {:5.2f} {:5.2f}".format(*countryWeather))
    print ()

    """ Filter table to southeastern semi-hemisphere records, group by country,
        and aggregate min, max, and mean on the average temperature; using the default
        binary decoding and the base GPUdb interface. """
    h_db.filter(table_name = weather_table_name, view_name = weather_se_view, expression="x > 0 and y < 0")

    response = h_db.aggregate_group_by( table_name = weather_se_view,
                                        column_names = ["country", "min(avg_temp)", "max(avg_temp)", "mean(avg_temp)"],
                                        offset = 0, limit = 25 )
    data = h_db.parse_dynamic_response(response)['response']
    print ( "{:<20s} {:^5s} {:^5s} {:^5s}".format("SE Semi-Hemi Country", "Min", "Mean", "Max"))
    print ( "{:=<20s} {:=<5s} {:=<5s} {:=<5s}".format("", "", "", ""))
    for countryWeather in zip(data["country"], data["min(avg_temp)"], data["mean(avg_temp)"], data["max(avg_temp)"]):
        print ( "{:<20s} {:5.2f} {:5.2f} {:5.2f}".format(*countryWeather))
    print ()

    ### Aggregate Example 5
    
    """ Filter for southern hemisphere cities and create a histogram for the
        average temperature of those cities (divided into every 10 degrees,
        e.g., 40s, 50s, 60s, etc.) """
    s_view = weather_table.filter( expression = "y < 0" )

    histogram_result = s_view.aggregate_histogram( column_name = "avg_temp",
                                                   start = 40, end = 90,
                                                   interval = 10 )
    print ( "Number of southern hemisphere cities with average temps in the given ranges:")
    for histogroup in zip([40, 50, 60, 70, 80], histogram_result['counts']):
        print ( "\t{}s: {:2.0f}".format(*histogroup))
    print()


    ### Aggregate Example 6

    """ Aggregate group by has an option 'result_table' which creates a result table and does not
        return the data.  Very useful when the data is large and we want to fetch records from it
        in batches.
    """
    # Create another table with the same type, and generate a lot of random data for it.
    # Note that we're allowing GPUdbTable to come up with a random name for the table.
    weather_table2 = gpudb.GPUdbTable( columns, db = h_db )
    # Create random data (but specify a range for the average temperature column)
    weather_table2.insert_records_random( count = 10000,
                                          options = { "avg_temp": {"min": -20, "max": 105 } } )
    print()
    print ( "Second weather table size: ", weather_table2.size() )

    # Create a view on the south-western quadrant of the planet
    sw_view = weather_table2.filter( expression="x < 0 and y < 0" )
    # Then do the aggregation operation .  Note that the column names need
    # aliases to utilize th 'result_table' option.
    agg_result_table = sw_view.aggregate_group_by( column_names = ["country",
                                                                   "min(avg_temp) as min_avg_temp",
                                                                   "max(avg_temp) as max_avg_temp",
                                                                   "mean(avg_temp) as mean_avg_temp"],
                                                   offset = 0, limit = 25,
                                                   options = { "result_table": gpudb.GPUdbTable.prefix_name("agg_") } )
    print ( "Size of records in the SW quadrant of the planet: ", agg_result_table.size() )
    print ( "{:<20s} {:^7s} {:^7s} {:^5s}".format("SW Semi-Hemi Country", "Min", "Mean", "Max"))
    print ( "{:=<20s} {:=<6s} {:=<6s} {:=<6s}".format("", "", "", ""))

    # Note that we can slice GPUdbTable objects to fetch the data inside
    for record in agg_result_table[ 10 : 50 ]:
        print ( "{:<20s} {:5.2f} {:5.2f} {:5.2f}".format( record["country"], record["min_avg_temp"], record["mean_avg_temp"], record["max_avg_temp"] ))
    print ()
    

    print ( "\n")
    print ( "DELETING DATA")
    print ( "-------------")
    print ()

    """ Filter for cities that are either south of latitude -50 or west of
        longitude -50 to determine how many records will be deleted; delete
        the records, then confirm the deletion by refiltering. """

    deleteExpression = "x < -50 or y < -50"
    num_records_to_delete = weather_table.filter( expression = deleteExpression ).count
    print ( "Number of records that meet deletion criteria before deleting:  {}"
            "".format( num_records_to_delete ) )

    weather_table.delete_records( expressions = [ deleteExpression ] )

    # Note that we're using the duplicate GPUdbTable object which points to the
    # same table in the DB
    num_records_post_delete = weather_table_duplicate.filter( expression = deleteExpression ).count
    print ( "Number of records that meet deletion criteria after deleting (expect 0):  {}".format( num_records_post_delete ))
    print ()



    print ( "\n")
    print ( "Using Multi-head Ingestion")
    print ( "--------------------------")
    print ()

    """For tables with primary or shard key columns, it might be useful to use
       the multi-head ingestion procedure for inserting records into a table for
       heavy ingestion loads.  There are benefits and drawbacks of using multi-head
       ingestion: the benefit is that if the database is configured for multi-head
       ingestion and there is a tremendous ingestion load, then the ingestion will
       be faster over all.  However, the drawback is that the client has to do some
       calculation PER record to find out which worker rank of the database server to
       send the record to.  So, unless the following parameters are met, it is unwise
       to use multi-head ingestion as it will unncessarily slow ingestion down:

       * The server is configured to use multi-head ingestion
       * The table type has at least one primary or shard key column
       * There is a heavy stream of data to be inserted
    """
    # Create a type that has some shard keys
    sharded_columns = [
        [ "city", "string", "char16" ],
        [ "state_province", "string", "char2", "shard_key" ],  # shard key column
        [ "country", gpudb.GPUdbRecordColumn._ColumnType.STRING, gpudb.GPUdbColumnProperty.CHAR16 ],
        [ "airport", "string", "nullable" ], # a nullable column
        [ "x", "double" ],
        [ "y", "double" ],
        [ "avg_temp", "double" ],
        [ "time_zone", "string", "char8", "shard_key" ] # shard key column
    ]

    # Create a table with the multi-head ingestion options
    # (the default batch size is 10k)
    sharded_table = gpudb.GPUdbTable( sharded_columns, db = h_db,
                                      use_multihead_ingest = True,
                                      multihead_ingest_batch_size = 33 )

    # Generate some random data to be inserted
    num_records = 100
    null_likelihood = 10
    alphanum = (string.ascii_letters + string.digits)
    for i in range(0, num_records):
        record = collections.OrderedDict()
        record[ "city"          ] = ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 5, 16 ) )] )
        record[ "state_province"] = ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 2 ) )] )
        record[ "country"       ] = ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 5, 16 ) )] )
        record[ "airport"       ] = None if (random.random() < null_likelihood) \
                                    else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 2, 25 ) )] )
        record[ "x"             ] = random.uniform( -180, 180 )
        record[ "y"             ] = random.uniform(  -90,  90 )
        record[ "avg_temp"      ] = random.uniform(  -40, 110 )
        record[ "time_zone"     ] = "UTC-{}".format( random.randint( -11, 14 ) )
        sharded_table.insert_records( record )
    # end loop

    print ( "Size of sharded table (expect less than 100 as the batch size is 33 and \n100 is not a multiple of 33): ", sharded_table.size() )
    print ()
    print ( "Flushing the records remaining in the ingestor queue...")
    sharded_table.flush_data_to_server()
    print ( "Size of sharded table post forced flush (expect 100): ", sharded_table.size() )
    print ()

    
    
# end gpudb_example()


if __name__ == '__main__':
    gpudb_example()
