# GPUdb Python API Changelog

## Version 7.0

### Version 7.0.20.1 - 2022-08-31

#### Fixed
-   Memory leak in underlying C-extension library


### Version 7.0.20.0 - 2020-11-25

#### Added
-   GPUdb methods for adding custom headers per endpoint call:
    -   ``add_http_header()``
    -   ``remove_http_header()``
    -   ``get_http_headers()``

#### Changed
    GPUdb Table Monitor API
-   The GPUdb table monitor API has been refactored to be more user friendly.
    Here are the following breaking changes (as compared to the initial API
    released in version 7.0.17.0):
    -   The only exported class is ``GPUdbTableMonitor`` which acts as a global
        namespace.  It contains the following nested public classes that the
        user will use:
    -   ``GPUdbTableMonitor.Client``
    -   ``GPUdbTableMonitor.Options``
    -   ``GPUdbTableMonitor.Callback``
        -   ``GPUdbTableMonitor.Callback.Options`` (and its derivative
            ``InsertDecodedOptions``)
        -   ``GPUdbTableMonitor.Callback.Type``
    -   The old ``GPUdbTableMonitorBase.Options`` is now
        ``GPUdbTableMonitor.Options``.  All old options have been replaced
        with following current option:
        -   ``inactivity_timeout``
    -   The following classes have been refactored or removed (users need not
        use the refactored versions; they exist only for internal purposes):
        -   ``GPUdbTableMonitorBase``
        -   ``BaseTask``
        -   ``InsertWatcherTask``
        -   ``DeleteWatcherTask``
        -   ``UpdateWatcherTask``
        -   ``TableEvent``
        -   ``NotificationEventType``
        -   ``NotificationEvent``
        -   ``TableEventType``


#### Note
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.0.19.0 - 2020-08-24

#### Note
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.0.18.0 - 2020-07-30

#### Changed

-   Some GPUdbTable methods to have a limit of -9999, instead of 10,000, to
    align it with the GPUdb method default value:
    -   ``get_records()``
    -   ``get_records_by_column()``
    -   ``get_records_from_collection()``
    -   ``get_geo_json()``

#### Note
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.0.17.0 - 2020-07-06

#### Added

-   Introduced new API for client side table monitor feature.
    The name of the class to use is `GPUdbTableMonitorBase`
    A default implementation which demonstrates usage of this class is
    included in the class `GPUdbTableMonitor`.
    A full example is found in the directory `examples` in the Python file
    `table_monitor_example_Default_impl.py`.
    There are other variants of the example in the Python files,
    `table_monitor_example_basic_first.py` and
    `table_monitor_example_basic_second.py`.
    Several test cases are included in the `test/table_monitor` directory.


#### Changed

-   GPUdbTable.random_name() and GPUdbTable.prefix_name() now generate
    strings without hyphens to be more SQL-compatible.


### Version 7.0.16.0 - 2020-05-28

#### Fixed
-   GPUdbTable.alter_table() now updates the multi-head I/O objects' table
    names so that subsequent multi-head I/O operations work.
-   Response encoding for GPUdb endpoints that return encoded data for
    JSON encoding.
-   Occassional data corruption issue with replacing '\U' with '\u' for
    JSON encoding.


### Version 7.0.15.4 - 2020-05-18

#### Fixed
-   Python 3 compatibility issue in GPUdbIngestor


### Version 7.0.15.3 - 2020-05-15

#### Fixed
-   GPUdbIngestor bug introduced in 7.0.15.2 :-/


### Version 7.0.15.2 - 2020-05-13

#### Fixed
-   GPUdbTable.insert_records() slow-down when using multi-head ingestion.


### Version 7.0.15.1 - 2020-05-07

#### Fixed
-   GPUdbTable.insert_records() bug when using stringified numerics (e.g. "1"
    for an integer column) with multi-head ingestion.


### Version 7.0.15.0 - 2020-04-27

#### Fixed
-   GPUdbTable creation--type comparison issue where column data type case
    is not all lower case and the column properties' order and case are
    different from the existing type.

#### Note
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.0.14.1 - 2020-03-31

#### Changed
-   Upon client-server version mismatch, do not throw an exception anymore.
    Just log a warning.


### Version 7.0.14.0 - 2020-03-25

#### Note
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.



### Version 7.0.13.0 - 2020-03-10

#### Changed
-   GPUdb constructor behavior--if a single URL is used and no primary URL
    is specified via the options, the given single URL will be treated as
    the primary URL.

#### Note
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.0.12.0 - 2020-02-09

#### Added
-   GPUdbRecordType property `column_names` which is a list containing the
    names of all the columns in the record type, in the order they appear
    in the type.
-   GPUdbRecordType method `get_column()` that takes a string (column name)
    or integer (column index) and returns the respective `GPUdbRecordColumn`
    object.

#### Fixed
-   `GPUdbTable.get_records_by_key()` issue dict as the input argument

#### Note
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.0.11.0 - 2019-12-10

#### Added
-   Support for overriding the high availability synchronicity mode for
    endpoints; set the property `ha_sync_mode` of gpudb.GPUdb with one of
    the following values of the enumeration gpudb.GPUdb.HASynchronicityMode:
    - DEFAULT
    - SYNCHRONOUS
    - ASYNCRHONOUS

#### Fixed
-   Example script to match changed endpoint format.

### Version 7.0.9.0 - 2019-09-24

#### Added
-   Support for high-availability failover when the database is in the
    offline mode.


### Version 7.0.7.0 - 2019-08-29

#### Added
-   Support for new column property 'ulong' to multi-head I/O.  ***Compatible
    with Kinetica Server version 7.0.7.0 and later only.***
-   The following properties to GPUdbTable regarding whether the table itself is a
    collection or belongs to a collection:
    -   GPUdbTable.is_collection
    -   GPUdbTable.collection_name
-   GPUdb class constructor parameter `skip_ssl_cert_verification` which disables
    verifying the SSL certificate for the Kinetica server for HTTPS connections.

#### Fixed
-   Some Python3 compatibility related issues

#### Server Version Compatibilty
-   Kinetica 7.0.7.0 and later


### Version 7.0.6.1 - 2019-08-14

#### Changed
-   Added support for high availability failover when the system is limited
    (in addition to connection problems).  ***Compatible with Kinetica Server
    version 7.0.6.2 and later only.***

#### Server Version Compatibilty
-   Kinetica 7.0.6.2 and later


### Version 7.0.6.0 - 2019-07-27

#### Added
-   Support for passing /get/records options to RecordRetriever.get_records_by_key()
    and GPUdbTable.get_records_by_key()


### Version 7.0.5.0 - 2019-06-26

#### Added
-   Minor documentation and some options for some endpoints

#### Changed
-   Parameters for /visualize/isoschrone


### Version 7.0.4.0 - 2019-06-21

#### Changed
-   Lifted restrictions on columns with property date, time, datetime, or
    timestamp such that no validation occurs by the client.  This allows
    the `init_with_now` property to be applied to such columns.


### Version 7.0.1.1 - 2019-03-31
-   Changed GPUdbTable constructor behavior--it no longer calls /show/table
    with `get_sizes = true` since that can be a relatively costly query.
    __len__() now calls /show/table as needed, even for read-only tables.
-   Added option `is_automatic_partition` to GPUdbTableOptions


### Version 7.0.1.0 - 2019-03-12
-   Added support for selecting a primary host for the GPUdb class
-   Added support for high availability (HA) to multi-head ingestion
    retrieval


### Version 7.0.0.2 - 2019-02-26
-   Added some logging support

### Version 7.0.0.1 - 2019-02-09
-   Added support for high availability (HA) failover logic to the
    GPUdb class


### Version 7.0.0.0 - 2019-01-31
-   Added support for cluster reconfiguration to the multi-head I/O operations


## Version 6.2

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
