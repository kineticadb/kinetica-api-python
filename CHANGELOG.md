# GPUdb Python API Changelog

## Version 7.0.1

### Version 7.0.1.1 - 2019-03-31
-   Changed GPUdbTable constructor behavior--it no longer calls /show/table
    with `get_sizes = true` since that can be a relatively costly query.
    __len__() now calls /show/table as needed, even for read-only tables.
-   Added option `is_automatic_partition` to GPUdbTableOptions


### Version 7.0.1.0 - 2019-03-12
-   Added support for selecting a primary host for the GPUdb class
-   Added support for high availability (HA) to multi-head ingestion
    retrieval



## Version 7.0.0

### Version 7.0.0.2 - 2019-02-26
-   Added some logging support

### Version 7.0.0.1 - 2019-02-09
-   Added support for high availability (HA) failover logic to the
    GPUdb class


### Version 7.0.0.0 - 2019-01-31
-   Added support for cluster reconfiguration to the multi-head I/O operations


## Version 6.2.0

### Version 6.2.0.11 - 2019-03-22
-   Changed GPUdbTable constructor behavior--it no longer calls /show/table
    with `get_sizes = true` since that can be a relatively costly query.
    __len__() now calls /show/table as needed, even for read-only tables.


### Version 6.2.0.10 - 2018-09-16
-   Added support for host manager endpoints
-   Added support for replicated tables to multi-head ingestion via GPUdbTable
    and GPUdbIngestor
-   Added head-node only usage support to GPUdbWorkerList


### Version 6.2.0.0 - 2018-05-09
-   New RecordRetriever class to support multi-head record lookup by
    shard key in gpudb_multihead_io.py (file previously named gpudb_ingestor.py)
-   Renamed gpudb_ingestor.py to gpudb_multihead_io.py
-   Added an opt-out mechanism for the GPUdb constructor such that no version check
    or other communication is made with the server. 
-   Added an in-house compiled C-module named protcol for avro encoding and
    decoding to drastically increase record ingestion and retrieval speed.
-   Added a convenience method get_geo_json() to GPUdbTable that returns a
    GeoJSON object from a table.



## Version 6.1.0 - 2017-10-05

-   Added new GPUdbTable class that makes the creation of tables and data i/o
    much more convenient.  Query chaining for relevant endpoints is also made
    convenient.

-   Added object oriented record type and data handling.  See new classes
    * GPUdbColumnProperty -- Contains the different column properties.
    * GPUdbRecordColumn -- Represents a column for any given record.
    * GPUdbRecordType -- Represents a record data type.  Has convenient functions
                         for creating a type in GPUdb.
    * GPUdbRecord -- Contains data for any given record; also has convenient
                     data encoding and decoding functions for inserting
                     and fetching data from the database server.


## Version 6.0.0 - 2017-01-24

-   Releasing version



## Version 5.4.0 - 2016-11-29

-   Releasing version



## Version 5.2.0 - 2016-06-25

-   Maintenance



## Version 5.2.0 - 2016-06-25

-   Added the pymmh3 python package.



## Version 5.1.0 - 2016-05-06

-   Updated documentation generation.



## Version 4.2.0 - 2016-04-11

-   Refactor generation of the APIs.
