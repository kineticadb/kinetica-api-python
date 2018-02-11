from __future__ import print_function


from gpudb import GPUdb, GPUdbTable, GPUdbRecordType
from gpudb_multihead_io import GPUdbIngestor, GPUdbWorkerList
import datetime
import json
import random
import string
import sys
import time
from multiprocessing import Pool

from avro import schema, datafile, io

if sys.version_info >= (2, 7):
    import collections
else:
    import ordereddict as collections # a separate package


# Override datetime's strftime which in python does not accept
# years before 1900--annoying!

import re, time

# remove the unsupposed "%s" command.  But don't
# do it if there's an even number of %s before the s
# because those are all escaped.  Can't simply
# remove the s because the result of
#  %sY
# should be %Y if %s isn't supported, not the
# 4 digit year.
_illegal_s = re.compile(r"((^|[^%])(%%)*%s)")

def _findall(text, substr):
     # Also finds overlaps
     sites = []
     i = 0
     while 1:
         j = text.find(substr, i)
         if j == -1:
             break
         sites.append(j)
         i=j+1
     return sites
# end _findall


# Every 28 years the calendar repeats, except through century leap
# years where it's 6 years.  But only if you're using the Gregorian
# calendar.  ;)

def strftime(dt, fmt):
    if _illegal_s.search(fmt):
        raise TypeError("This strftime implementation does not handle %s")
    if dt.year > 1900:
        return dt.strftime(fmt)

    year = dt.year
    # For every non-leap year century, advance by
    # 6 years to get into the 28-year repeat cycle
    delta = 2000 - year
    off = 6*(delta // 100 + delta // 400)
    year = year + off

    # Move to around the year 2000
    year = year + ((2000 - year)//28)*28
    timetuple = dt.timetuple()
    s1 = time.strftime(fmt, (year,) + timetuple[1:])
    sites1 = _findall(s1, str(year))
    
    s2 = time.strftime(fmt, (year+28,) + timetuple[1:])
    sites2 = _findall(s2, str(year+28))

    sites = []
    for site in sites1:
        if site in sites2:
            sites.append(site)
            
    s = s1
    syear = "%4d" % (dt.year,)
    for site in sites:
        s = s[:site] + syear + s[site+4:]
    return s
# end strftime

# ----------- end override ------------------





# global variable needed for multiprocessing
gpudb_ingestor = None



def test_gpudb_ingestor():
    """Tries to stress out Kinetica's multi-head ingestion mode.  Tests
       all possible sharding under the sun.
    """
    global gpudb_ingestor

    gpudb = GPUdb( encoding='BINARY', host = '127.0.0.1', port = '9191' )

    table_name = "test_ingest_table2"

    # Clear table if exists
    gpudb.clear_table( table_name, options = {"no_error_if_not_exists": "true"} )

    # The table type/schema-- want all possibly type/properties to be sharded and nullable
    _type = [ ["i1",          "int"                                       ],
              ["i2",          "int", "shard_key", "nullable"              ],
              ["i8",          "int", "shard_key", "nullable", "int8"      ],
              ["i16",         "int", "shard_key", "nullable", "int16"     ],
              ["d1",       "double", "shard_key", "nullable"              ],
              ["f1",        "float", "shard_key", "nullable"              ],
              ["l1",         "long", "shard_key", "nullable"              ],
              ["timestamp",  "long", "shard_key", "nullable", "timestamp" ],
              ["s1",       "string", "shard_key", "nullable"              ],
              ["date",     "string", "shard_key", "nullable", "date"      ],
              ["datetime", "string", "shard_key", "nullable", "datetime"  ],
              ["decimal",  "string", "shard_key", "nullable", "decimal"   ],
              ["ipv4",     "string", "shard_key", "nullable", "ipv4"      ],
              ["time",     "string", "shard_key", "nullable", "time"      ],
              ["c1",       "string", "shard_key", "nullable", "char1"     ],
              ["c2",       "string", "shard_key", "nullable", "char2"     ],
              ["c4",       "string", "shard_key", "nullable", "char4"     ],
              ["c8",       "string", "shard_key", "nullable", "char8"     ],
              ["c16",      "string", "shard_key", "nullable", "char16"    ],
              ["c32",      "string", "shard_key", "nullable", "char32"    ],
              ["c64",      "string", "shard_key", "nullable", "char64"    ],
              ["c128",     "string", "shard_key", "nullable", "char128"   ],
              ["c256",     "string", "shard_key", "nullable", "char256"   ] ]
    table = GPUdbTable( _type, table_name, db = gpudb )

    print ("Table Name:", table_name)

    record_type = table.get_table_type()


    # Instantiate a gpudb ingestor object; pay attention to the batch size.
    # Realistic cases would have higher batch sizes.
    ingestor_batch_size = 200
    options = {}
    workers = GPUdbWorkerList( gpudb )
    print ("Workers: ", workers.worker_urls, "\n")
    gpudb_ingestor = GPUdbIngestor( gpudb, table_name, record_type, ingestor_batch_size, options, workers )

    # Generate records to insert
    num_batches      =    5  # Passed to generate_and_insert_data()
    batch_size       = 1000  # Passed to generate_and_insert_data()
    num_pools        =    5  # Number of threads spawned in a single Pool call
    num_pool_batches =   10  # Number of times Pool is invoked

    # # In case someone wants to call the function directly
    # generate_and_insert_data( [batch_size, num_batches] ) # debug~~~~~~~~~~~~
    
    # Generate and insert data parallelly; total number of processes
    # spawned: (num_pools * num_pool_batches)
    for i in range(0, num_pool_batches):
        pool = Pool( processes = num_pools )
        results = pool.map_async( generate_and_insert_data, [[batch_size, num_batches]] * num_pools)
        results.get()
        pool.close()
        pool.join()
    # end multithreaded data generation and insertion

    # # Flush the ingestor
    # # NOTE: Was not seeing any record in the queues due to python's
    # # multithreading issues... need to flush from the function below
    # gpudb_ingestor.flush()

    num_records = num_batches * batch_size * num_pools * num_pool_batches
    print ()
    print ("Table name:", table_name)
    print ("Total # objects inserted:", num_records)
    print ()
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

    null_percentage = 0.1
    alphanum = (string.ascii_letters + string.digits)

    # Nested loop
    # Outer loop controls how many batches of records are added to the ingestor
    for i in range(0, num_batches):
        print ("thread {_id:>5} outer loop: {i:>5}".format( _id = my_id, i = i ))
        records = []
        # Inner loop generated records for this batch
        for j in range(0, batch_size):
            _i_plus_j = (i + j)
            record = collections.OrderedDict()
            record[ "i1"  ] = i * j
            record[ "i2"  ] = random.randint( -_i_plus_j, _i_plus_j ) if (random.random() >= null_percentage) else None
            record[ "i8"  ] = random.randint( -128, 127 ) if (random.random() >= null_percentage) else None
            record[ "i16" ] = random.randint( -32768, 32767 ) if (random.random() >= null_percentage) else None
            record[ "d1"   ] = (random.random() * _i_plus_j ) if (random.random() >= null_percentage) else None
            record[ "f1"   ] = (random.random() * _i_plus_j ) if (random.random() >= null_percentage) else None
            record[ "l1"   ] = (random.randint( 0,_i_plus_j ) * _i_plus_j ) if (random.random() >= null_percentage) else None
            record[ "timestamp" ] = random.randint( -30610239758979, 29379542399999 ) if (random.random() >= null_percentage) else None
            record[ "s1"   ] = None if (random.random() < null_percentage) \
                               else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 2, 200 ) )] )
            record[ "date" ] = None if (random.random() < null_percentage) \
                               else strftime( datetime.date( random.randint( 1000, 2900 ), # year
                                                             random.randint( 1, 12 ), # month
                                                             random.randint( 1, 28 ) # day
                                                         ), "%Y-%m-%d" )
            record[ "datetime" ] = None if (random.random() < null_percentage) \
                                   else ( strftime( datetime.date( random.randint( 1000, 2900 ), # year
                                                                 random.randint( 1, 12 ), # month
                                                                 random.randint( 1, 28 ) # day
                                                             ), "%Y-%m-%d" ) \
                                          + " "
                                          + ( datetime.time( random.randint( 0, 23 ), # hour
                                                             random.randint( 0, 59 ), # minute
                                                             random.randint( 0, 59 ) # seconds
                                                         ).strftime( "%H:%M:%S" ) )
                                          + (".%d" % random.randint( 0, 999 ) ) )  # milliseconds
            record[ "decimal" ] = None if (random.random() < null_percentage) \
                                  else ( str( random.randint( -922337203685477, 922337203685477 ) )
                                         + "." + str( random.randint( 0, 9999 ) ) )
            record[ "ipv4" ] = None if (random.random() < null_percentage) \
                               else '.'.join( [ str( random.randint( 0, 255 ) ) for n in range(0, 4)] )
            record[ "time" ] = None if (random.random() < null_percentage) \
                               else ( datetime.time( random.randint( 0, 23 ), # hour
                                                     random.randint( 0, 59 ), # minute
                                                     random.randint( 0, 59 ) # seconds
                                                 ).strftime( "%H:%M:%S" ) \
                                      + (".%d" % random.randint( 0, 999 ) ) )  # milliseconds
            record[ "c1"  ] = None if (random.random() < null_percentage) \
                              else random.choice( alphanum )
            record[ "c2"  ] = None if (random.random() < null_percentage) \
                              else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 2 ) )] )
            record[ "c4"  ] = None if (random.random() < null_percentage) \
                              else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 4 ) )] )
            record[ "c8"  ] = None if (random.random() < null_percentage) \
                              else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 8 ) )] )
            record[ "c16" ] = None if (random.random() < null_percentage) \
                              else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 16 ) )] )
            record[ "c32" ] = None if (random.random() < null_percentage) \
                              else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 32 ) )] )
            record[ "c64" ] = None if (random.random() < null_percentage) \
                              else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 64 ) )] )
            record[ "c128"] = None if (random.random() < null_percentage) \
                              else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 128 ) )] )
            record[ "c256"] = None if (random.random() < null_percentage) \
                              else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 256 ) )] )

            # Add the record to the list of records
            records.append( record )
        # end for loop

        # Add the records to the ingestor
        gpudb_ingestor.insert_records( records )
    # end generating data


    # Need to flush here since the gpudb_ingestor of the parent
    # thread won't get this child thread's state
    gpudb_ingestor.flush()
# end generate_and_insert_data



if __name__ == '__main__':
    start_time = time.time()

    test_gpudb_ingestor()

    end_time = time.time()
    elapsed_time = (end_time - start_time)

    print ()
    print ("***************************************")
    print ("*******Total elapsed time: %0.6f" % elapsed_time)
    print ("***************************************")
    print ()




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
