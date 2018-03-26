GPUdb Python API Changelog
==========================

Version 6.2.0 - 2018-03-25
--------------------------

-   New RecordRetriever class to support multi-head record lookup by
    shard key in gpudb_multihead_io.py (file previously named gpudb_ingestor.py)
-   Renamed gpudb_ingestor.py to gpudb_multihead_io.py
-   Added an opt-out mechanism for the GPUdb constructor such that no version check
    or other communication is made with the server. 
-   Added an in-house compiled C-module named protcol for avro encoding and
    decoding to drastically increase record ingestion and retrieval speed.


Version 6.1.0 - 2017-10-05
--------------------------

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


Version 6.0.0 - 2017-01-24
--------------------------

-   Releasing version


Version 5.4.0 - 2016-11-29
--------------------------

-   Releasing version


Version 5.2.0 - 2016-06-25
--------------------------

-   Maintenance


Version 5.2.0 - 2016-06-25
--------------------------

-   Added the pymmh3 python package.


Version 5.1.0 - 2016-05-06
--------------------------

-   Updated documentation generation.


Version 4.2.0 - 2016-04-11
--------------------------

-   Refactor generation of the APIs.
