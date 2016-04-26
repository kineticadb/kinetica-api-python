from gpudb import GPUdb
from gpudb_ingestor import GPUdbIngestor
import json
import random
import sys
import time
from multiprocessing import Pool

from avro import schema, datafile, io

if sys.version_info >= (2, 7):
    import collections
else:
    import ordereddict as collections # a separate package


# global variable needed for multiprocessing
gpudb_ingestor = None

def test_gpudb_ingestor():
    global gpudb_ingestor

    gpudb = GPUdb( encoding='BINARY', host = '127.0.0.1', port = '9191')

    table_name = "test_ingest_table"
    # Clear table if exists
    gpudb.clear_table( table_name )

    # Create the table schema and the table
    table_type_schema_json = {
        "type": "record",
        "name": "ingest_test_type",
        "fields" :
        [
            { "name" : "d1", "type": "double" },
            { "name" : "d2", "type": "double" },
            { "name" : "l", "type": "long" },
            { "name" : "s", "type": "string" }
        ]
    }
    table_type_schema_str = json.dumps( table_type_schema_json )
    table_type_schema = schema.parse( table_type_schema_str )
    # Column names
    d1 = "d1"
    d2 = "d2"
    l  = "l"
    s  = "s"

    table_column_properties = {}

    type_id = gpudb.create_type( type_definition = table_type_schema_str,
                                 label = "",
                                 properties = table_column_properties )[ "type_id" ]
    
    gpudb.create_table( table_name = table_name,
                        type_id = type_id, )

    print "Table Name:", table_name

    # Instantiate a gpudb ingestor object
    batch_size = 7000
    options = {}
    # workers = None
    workers = GPUdbIngestor.WorkerList( gpudb )
    print "Workers: ", workers.worker_urls, "\n" 
    gpudb_ingestor = GPUdbIngestor( gpudb, table_name, batch_size, options, workers )

    # Generate records to insert
    num_batches =   50
    batch_size  = 10000
    num_pools = 5
    num_pool_batches = 5

    # Generate and insert data parallelly in a pool of 5
    for i in range(0, num_pool_batches):
        pool = Pool( processes = num_pools )
        results = pool.map_async( generate_and_insert_data, [[batch_size, num_batches]] * num_pools)
        results.get()
        pool.close()
        pool.join()
    # end multithreaded data generation and insertion



    # Flush the ingestor
    gpudb_ingestor.flush()

    num_records = num_batches * batch_size * num_pools * num_pool_batches
    print
    print "Total # objects inserted:", num_records
    print
# end test_gpudb_ingestor


def generate_and_insert_data( inputs ):
    """Given a batch size, generate a certain kind of record data and
    insert into the gpudb ingestor.

    @param gpudb_ingestor  The multihead ingestor for GPUdb.
    @param batch_size  The number of records per batch.
    @param num_batches  How many batches to create and insert.
    """
    global gpudb_ingestor

    batch_size, num_batches = inputs

    my_id = int(random.random() * 100)

    for i in range(0, num_batches):
        print "thread {_id:>5} outer loop: {i:>5}".format( _id = my_id, i = i )
        for j in range(0, batch_size):
            _i_plus_j = (i + j)
            record = collections.OrderedDict()
            record[ "d1" ] = i * j * 1.0
            record[ "d2" ] = _i_plus_j * 0.2
            record[ "l" ] = (i % 100)
            record[ "s" ] = str( _i_plus_j % 100 )

            # Add the record to the ingestor
            gpudb_ingestor.insert_record( record )
    # end generating data
# end generate_and_insert_data



if __name__ == '__main__':
    start_time = time.time()

    test_gpudb_ingestor()

    end_time = time.time()
    elapsed_time = (end_time - start_time)

    print
    print "***************************************"
    print "*******Total elapsed time: %0.6f" % elapsed_time
    print "***************************************"
    print




# Parallel process with multihead ingest of three ranks for ingestion
#
# Total # objects inserted: 12500000
#
#
# ***************************************
# *******Total elapsed time: 264.172765
# ***************************************


# Parallel process with single-head ingest (the winner!!)
#
# Total # objects inserted: 12500000
#
#
# ***************************************
# *******Total elapsed time: 260.663663
# ***************************************
