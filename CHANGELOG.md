# GPUdb Python API Changelog

## Version 7.2


### Version 7.2.3.3 - 2026-01-09

#### Added
-   Support for a user-specified default schema when using the DBAPI
-   Support for user-specified HTTP headers when using the DBAPI

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.3.2 - 2025-11-17

#### Added
-   Support for 12-byte decimals
-   Support for unsigned long array types

#### Changed
-   Improved efficiency in processing column properties
-   Improved & aligned handling of connection errors across endpoint types

#### Fixed
-   Check of HA queue draining status on fail-back

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.3.1 - 2025-10-07

#### Changed
-   Improved connection management
-   Returned parse failure reason from URL parser
-   Pool size configuration of `requests` set to defaults


### Version 7.2.3.0 - 2025-09-15

#### Added
-   Logging to the DBAPI library

#### Changed
-   Switched fail-back poller to use status call to determine HA queue draining
    state
-   Removed additional Python2 handling

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.2.15 - 2025-11-12

#### Added
-   Support for unsigned long array types

#### Changed
-   Improved consistency of connection error handling


### Version 7.2.2.14 - 2025-10-08

#### Changed
-   Improved connection management
-   Returned parse failure reason from URL parser
-   Pool size configuration of `requests` set to defaults


### Version 7.2.2.13 - 2025-09-12

#### Added
-   Insert/update counts for multi-head ingest calls
-   Simplified DML (insert, update, & delete) functions
-   Example programs for working with date/time objects, multi-head insert
    counts, and simplified DML functions

#### Changed
-   HTTP library from `httpx` to `requests`
-   Improved handling of time and null value inserts
-   Removed Python2 handling in multi-head I/O module

#### Fixed
-   Issue with invoking `GPUdbIngestor` directly without passing in options


### Version 7.2.2.12 - 2025-08-18

#### Fixed
-   Issue with `GPUdbSqlIterator` iterating over a batch size that is greater
    than the configured `max_get_records_size` on the server

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.2.11 - 2025-08-03

#### Fixed
-   Issue with `skip_ssl_cert_verification` parameter being ignored
-   Connectivity in environments where anonymous pass-through in httpd is
    blocked


### Version 7.2.2.10 - 2025-08-01

#### Added
-   Better request retry handling

#### Changed
-   Switched HTTP library from `httplib` to `httpx`
-   Updated installation instructions


### Version 7.2.2.9 - 2025-05-27

#### Changed
-   Improved support for DataFrame handling & type conversion
-   Improved destruction of HA fail-back poller object
-   Switched to non-deprecated SSL protocol constant

#### Fixed
-   Issue with using `GPUdbSqlIterator` on certain queries that return an
    unexpected response schema

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.2.8 - 2025-05-08

#### Added
-   Support for fail-back when primary cluster is initially down


### Version 7.2.2.7 - 2025-02-13

#### Added
-   Support for parallel HA modes

#### Changed
-   Removed logging of fail-back poller shutdown in `GPUdb` destructor


### Version 7.2.2.6 - 2025-02-06

#### Added
-   Fail back to a primary cluster after failing over to a secondary cluster


### Version 7.2.2.5 - 2025-01-21

#### Added
-   Check for a blocked table monitor port in `GPUdbTableMonitor` invocations
-   Configurable table monitor port


### Version 7.2.2.4 - 2025-01-06

#### Added
-   Support for inserting Record type objects


### Version 7.2.2.3 - 2024-11-20

#### Fixed
-   Builds for Mac on ARM


### Version 7.2.2.2 - 2024-11-10

#### Fixed
-   Handling of connections when connection keep-alive is active


### Version 7.2.2.1 - 2024-10-30

#### Fixed
-   Numeric query parameter handling to ignore tokens in quoted strings


### Version 7.2.2.0 - 2024-10-01

#### Fixed
-   Warnings related to escaped regular expression codes


### Version 7.2.1.0 - 2024.09.08

#### Added
-   OAuth2 authentication support
-   Support for SQLAlchemy integration

#### Changed
-   Upgraded internal `tabulate` to 0.9.0


### Version 7.2.0.12 - 2024-08-07

#### Changed
-   Improved handling of null vectors in DataFrames


### Version 7.2.0.11 - 2024-07-08

#### Changed
-   Completed asynchronous DB API interface


### Version 7.2.0.10 - 2024-06-17

#### Changed
-   Completed synchronous DB API interface; added batch insert handling


### Version 7.2.0.9 - 2024-06-02

#### Added
-   Python3 PEP 249 DB API interface

#### Changed
-   Minimum required Python version is 3.8
-   Project metadata moved to TOML format

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.2.0.8 - 2024-05-15

#### Fixed
-   Issue with Pandas DataFrame inserting timestamp value into
    `datetime` / `timestamp` column
-   Issue with `vector` containing integers failing insert


### Version 7.2.0.7 - 2024-05-07

#### Fixed
-   Issue with `as_json()` removing the password option


### Version 7.2.0.6 - 2024-04-22

#### Added
-   `GPUdbSqlContext` class for more easily creating SQL-GPT contexts
-   `GPUdb.get_connection()` function for connecting to a Kinetica database
    using credentials set as environment variables; useful for Jupyter notebooks


### Version 7.2.0.5 - 2024-04-15

#### Added
-   Server connection timeout parameter for handling timeouts for server status
    checks; this operates independently of the exiting timeout for user endpoint
    requests

#### Changed
-   Deprecated `GPUdb.is_kinetica_running()` function

#### Fixed
-   Handling of empty dataframes & strings in `GPUdbTable.from_df()`


### Version 7.2.0.4 - 2024-03-14

#### Fixed
-   Constant timeout reference for rank connections


### Version 7.2.0.3 - 2024-03-13

#### Added
-   Support for multiple column attribute overrides in `GPUdbTable.from_df()`

#### Changed
-   Increased connection timeout from 1 to 20 seconds to account for
    connections over high-traffic and public networks


### Version 7.2.0.2 - 2024.03.06

#### Added
-   Improved support for JSON & standard array input as Python list
    
#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.2.0.1 - 2024.02.22

#### Added
-   Improved support for vector input as Python list
-   Improved support for boolean array conversion
-   `GPUdbTable` option `convert_special_types_on_retrieval` for automatically
    converting array, JSON, & vector types to Python native types on retrieval
    
#### Fixed
-   Issue with null numeric columns


### Version 7.2.0.0 - 2024.02.12

#### Added
-   Support for array, JSON, & vector types
-   `GPUdbTableOptions` member `chunk_column_max_memory`

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.



## Version 7.1

### Version 7.1.10.1 - 2024-05-15

#### Added
-   Added error check for missing URL in `get_connection()`

#### Fixed
-   Issue with Pandas DataFrame inserting timestamp value into
    `datetime` / `timestamp` column


### Version 7.1.10.0 - 2024-05-07

#### Added
-   `GPUdbSqlContext` class for more easily creating SQL-GPT contexts
-   `GPUdb.get_connection()` function for connecting to a Kinetica database
    using credentials set as environment variables; useful for Jupyter notebooks


### Version 7.1.9.13 - 2024-04-15

#### Added
-   Server connection timeout parameter for handling timeouts for server status
    checks; this operates independently of the exiting timeout for user endpoint
    requests

#### Changed
-   Deprecated `GPUdb.is_kinetica_running()` function

#### Fixed
-   Handling of empty dataframes & strings in `GPUdbTable.from_df()`
-   Ability to use `gpudb_dataframe` & `gpudb_file_handler` modules in
    Python 2.7


### Version 7.1.9.12 - 2024-03-14

#### Fixed
-   Constant timeout reference for rank connections


### Version 7.1.9.11 - 2024-03-13

#### Added
-   Support for multiple column attribute overrides in `GPUdbTable.from_df()`

#### Changed
-   Increased connection timeout from 1 to 20 seconds to account for
    connections over high-traffic and public networks


### Version 7.1.9.10 - 2023-11-13

#### Fixed
-   Special characters in comments


### Version 7.1.9.9 - 2023-11-12

#### Added
-   New Pandas DataFrame functions


### Version 7.1.9.8 - 2023-10-26

#### Changed
-   Timeout in `get_system_properties` to user-defined (default infinite)


### Version 7.1.9.7 - 2023-10-12

#### Changed
-   Cleaned up unauthorized access exception handling
-   Dependency reference scheme modified to avoid collisions with user imports


### Version 7.1.9.6 - 2023-09-17

#### Added
-   Support for file upload & download from KiFS


### Version 7.1.9.5 - 2023-08-20

#### Added
-   Support for data egress in JSON format


### Version 7.1.9.4 - 2023-05-21

#### Added
-   Multi-head I/O support for Boolean column type


### Version 7.1.9.3 - 2023-04-30

#### Changed
-   Propagated connection logging level to `GPUdbIngestor` & `RecordRetriever`

#### Fixed
-   Multi-head I/O during a failover when using head node only


### Version 7.1.9.2 - 2023-04-23

#### Added
-   Batch and multi-head ingestion support for JSON-formatted data
-   Support for HA failover when user-specified connection URLs don't match the
    server-known URLs; multi-head operations will still be disabled

#### Changed
-   Removed N+1 features & references

#### Fixed
-   Several logging-related issues


### Version 7.1.9.1 - 2023-03-19

#### Added
-   Examples of secure/unsecure connections; improved SSL failure error message

#### Changed
-   `GPUdb` object construction will error out if connection attempts fail

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.1.8.4 - 2023-02-28

#### Changed
-   Move included `tabulate` library to avoid version conflicts


### Version 7.1.8.3 - 2023-01-26

#### Added
-   Added conversion to DataFrame functions:
    -  `GPUdb.to_df()` - convert SQL result set to DataFrame
    -  `GPUdbTable.to_df()` - convert table data to DataFrame


### Version 7.1.8.2 - 2022-12-14

#### Added
-   Re-added support for username/password in the URL
-   Re-added support for 7.0 protocol & port overrides
-   Re-added support for numeric log levels

#### Changed
-   Removed default HTTP protocol assignment in Options

#### Fixed
-   Result tables can now be created from GPUdbTable in schemas other than the
    one associated with the GPUdbTable table


### Version 7.1.8.1 - 2022-12-08

#### Added
-   Support for more varieties of Mac OS


### Version 7.1.8.0 - 2022-10-18

#### Fixed
-   Issue with injection of default HTTP port into HTTPS URLs
-   Issue with services that don't accept uppercase protocols
-   Issue with services that don't accept lowercase methods

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.1.7.6 - 2022-10-10

#### Added
-   Support for more platforms on Python 3.10

#### Fixed
-   Import issue with Python3 on Mac OS


### Version 7.1.7.5 - 2022-10-07

#### Fixed
-   Issue with injection of default HTTP port into HTTPS URLs


### Version 7.1.7.4 - 2022-10-06

#### Fixed
-   Issue with services that don't accept uppercase protocols


### Version 7.1.7.3 - 2022-08-31

#### Fixed
-   Memory leak in underlying C-extension library


### Version 7.1.7.2 - 2022-08-12

#### Fixed
-   Made the connection parameters more accommodating to additional host:port
    combinations


### Version 7.1.7.1 - 2022-08-10

#### Fixed
-   Made the connection parameters more accommodating to different URL/host/port
    combinations


### Version 7.1.7.0 - 2022-07-18

#### Fixed
-   Made the API Python3 compatible
-   Prevented client hanging when connection IP/URL does not match any known to
    the server; client will operate in degraded mode (no multi-head, etc.)
-   Removed client-side primary key check, to improve performance and make
    returned errors more consistently delivered
-   Rectified a formatting issue while building expressions for keyed lookups
    that was resulting in a failure on Python 2.7.x.
-   Corrected some string/null comparisons

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.1.6.1 - 2022-02-08

#### Fixed
-   Made 7.1.6 API backward-compatible with previous database versions


### Version 7.1.6.0 - 2022-01-27

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.1.5.0 - 2021-10-13

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.1.4.0 - 2021-07-29

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.1.3.5 - 2021-05-24

#### Fixed
-   GPUdbTable constructor error when the table exists in the database
    but the user does not have access to it.


### Version 7.1.3.4 - 2021-05-10

#### Fixed
-   Issue when server and client versions are incompatible resulting in
    an exception being thrown.  Now it prints a warning as expected.


### Version 7.1.3.3 - 2021-04-21

#### Fixed
-   Logging format millisecond so that it shows the actual millisecond, and not
    .333 always.


### Version 7.1.3.2 - 2021-04-16

#### Added
-   Class `GPUdb.Version` that represents Kinetica version (the server's
    or client's).
-   `GPUdb` properties:
    -   ``current_cluster_info``: `GPUdb.ClusterAddressInfo` object containing
                                  information on the active cluster.
    -   ``server_version``: `GPUdb.Version` containing the version of the active
                            server cluster's version, or None if not known.


### Version 7.1.3.1 - 2021-03-18

#### Fixed
-   Improved performance when debug or trace logging is not enabled


### Version 7.1.3.0 - 2021-03-05

#### Added
-   Added GPUdbTableMonitor to the API docs
-   Added GPUdbWorkerList to the API docs
-   Added GPUdbException to the API docs

#### Fixed
-   Improved class/method linkages in API docs
-   Issue with parsing a URL with no port

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.1.2.0 - 2021-01-25

#### Note
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.1.1.1 - 2020-12-16

#### Performance Enhancements
-   Increased overall multi-head I/O speed by reducing client-side
    workload.


### Version 7.1.1.0 - 2020-10-28

#### Added
-   GPUdb methods for adding custom headers per endpoint call:
    -   ``add_http_header()``
    -   ``remove_http_header()``
    -   ``get_http_headers()``
-   Add multi-head i/o support for new column type UUID
-   New log level ``trace`` for GPUdb

#### Changed

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


### Version 7.1.0.1 - 2020-08-27

#### Fixed
-   Added missing imports:
    -  Exception class `GPUdbUnauthorizedAccessException`
    -  Utility class `_Util`


### Version 7.1.0.0 - 2020-08-18

#### Added
-   GPUdbTable read-only property `qualified_table_name` which returns the
    fully qualified table name.
-   GPUdbTable static helper method `get_qualified_table_name` which returns the
    fully qualified version of the given table name.
-   GPUdb.HASynchronicityMode `NONE`
-   New exception classe(s):
    -   GPUdbHAUnavailableException (for internal API use)
    -   GPUdbFailoverDisabledException (for internal API use)
    -   GPUdbUnauthorizedAccessException
-   Method `is_kinetica_running()` for checking if a Kinetica instance is
    running at the given URL.
-   `GPUdb` read-only properties:
    -   all_cluster_info
    -   logging_level
    -   options
    -   protocol
-   Some convenience methods:
    -   GPUdb.ping()
    -   GPUdb.is_kinetica_running()
    -   GPUdb.get_server_debug_information()
    -   GPUdb.wms()
-   `GPUdbIngestor` read-only properties:
    -   retry_count

#### Changed
-   GPUdbTable methods that return a GPUdbTable object now creates that object
    based on a fully qualified (i.e. with schema) name which is returned in
    the response of the endpoint query.
-   `GPUdb.get_url()` takes an optional argument `stringified` with default value
    True.
-   `GPUdb.get_all_available_full_urls()` takes an optional argument
    `stringified` with default value True.
-   If the user gives the wrong host manager port to `GPUdb` via `GPUdb.Options`,
    `GPUdb` will no longer attempt to fix it.

#### Deprecated
-   The following `GPUdb` property setters have been deprecated; they will
    no longer have any effect on the GPUdb object:
    -   `GPUdb.connection`
    -   `GPUdb.host`
    -   `GPUdb.port`
    -   `GPUdb.host_manager_port`
    -   `GPUdb.gpudb_url_path`

#### Note
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.



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


### Version 7.0.9.0 - 2019-11-14

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

### Version 6.2.0.14 - 2019-08-29

#### Added
-   GPUdb class constructor parameter `skip_ssl_cert_verification` which disables
    verifying the SSL certificate for the Kinetica server for HTTPS connections.


### Version 6.2.0.13 - 2019-08-23

#### Added
-   The following properties to GPUdbTable regarding whether the table itself is a
    collection or belongs to a collection:
    -   GPUdbTable.is_collection
    -   GPUdbTable.collection_name

#### Fixed
-   Some Python3 compatibility related issues


### Version 6.2.0.12 - 2019-06-21

#### Changed
-   Lifted restrictions on columns with property date, time, datetime, or
    timestamp such that no validation occurs by the client.  This allows
    the `init_with_now` property to be applied to such columns.


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
