# GPUdb Python API Changelog

## Version 7.2


### Version 7.2.3.15 - 2026-11-18

#### Added
-   Multi-head ingest and key lookups reach each worker rank at any of the
    addresses the server reports for it, configured public addresses first,
    rather than only the first advertised one, so that they work for a
    cluster behind NAT, in a container or in Kubernetes.  A rank that answers
    at none of them makes the client use the head node alone.
    `GPUdb.Options.hostname_regex` restricts which addresses are tried.
-   Creating a `GPUdbTable` with multi-head I/O, a `GPUdbIngestor`, a
    `RecordRetriever` or a `GPUdbWorkerList` no longer checks every worker
    rank's address each time.  The connection checks them once when it
    connects to a cluster, fails over or fails back, and every object built
    on it uses that result.
-   `GPUdb.Options` for the HTTP transport:
    -   `tcp_keep_alive` (on by default; see Changed), with `tcp_keep_idle`,
        `tcp_keep_interval` and `tcp_keep_count` (0, the default, for the
        operating system's own).
    -   `trust_store_file_path`, a PEM file or c_rehash directory of CA
        certificates to verify HTTPS servers with; it used to take the
        `REQUESTS_CA_BUNDLE` environment variable.
    -   `max_connections_per_host`, the connections kept open for reuse per
        head or worker rank; it was fixed at 256, still the default.
-   `GPUdb.Options.use_snappy` and `GPUdb.use_snappy`, whether inserts and
    updates are compressed (see Changed).
-   The `fast` extra (`pip install gpudb[fast]`): `mmh3`, a C hash that
    halves the time to queue records for multi-head ingest, and
    `python-snappy`.  Without them, hashing is done in Python and nothing is
    compressed.
-   `GPUdb.ping()` with no URL pings the current cluster and fails over to the
    next if it cannot be reached or answers 500, 502, 503 or 504, raising a
    `GPUdbException` once every cluster has been tried.  A `timeout`
    argument, in seconds, bounds the wait.
-   Read-only accessors on `GPUdb`: `user_agent`, the User-Agent header this
    client sends with each request; `client_name` and `client_version`, as
    given in `GPUdb.Options`; `max_retries`, the number of times a failed
    request is retried; and `get_system_properties()`, the system properties
    of the cluster the client is using, as read when it connected.
-   `GPUdb.Version` keeps a fifth, `build`, component (a server's is a
    timestamp, as in 7.2.3.21.20260922210644); `full_version` gives all five,
    and `compare_to()`, `is_newer_than()`, `is_older_than()` and
    `is_equal_to()` compare with it.  The comparison operators compare the
    first four, as before.  `major`, `minor`, `revision` and `abi_version`
    read the components.
-   `get_errors()` and `get_warnings()` on `GPUdbIngestor` and `GPUdbTable`,
    which accumulate the record errors and warnings the server reports under
    the `return_individual_errors` insert option until called, at which point
    the notifications are returned and the respective list reset.
    `GPUdbTable.insert_records_response()` returns each call's response,
    errors included, without accumulating them.
-   `InsertionException.url` and `get_url()`, the URL the records were being
    sent to when the insertion failed, and `message`, the error message.
-   `GPUdbIngestor` options `json_options` (`validate` checks each JSON record
    as it is queued; off by default) and `check_permissions` (see Changed).
-   `GPUdbIngestor.close()`, and the ingestor as a context manager: closing
    flushes every queue, even after one fails, and the closed ingestor refuses
    records and flushes.  `closed` says whether it is closed.
-   `TypeObjectMap`, mapping a type to a Python class: `from_class()` makes the
    type from a dataclass's fields (`TypeObjectMap.column()` gives a field's
    column name, type and properties), and `from_type()` maps an existing
    type's columns to a class's attributes of the same name.  `to_object()`,
    `to_objects()` and `to_dict()` convert between records and objects,
    converting `bool`, `Decimal`, `datetime` (sent in UTC, converted from its
    time zone if it has one; decoded naive, in UTC), `date` and `time`
    values.  `create_type()` creates the type and registers the map.
-   `GPUdb.add_known_type()`, `add_known_type_from_table()`,
    `add_known_type_object_map()` and `get_known_type_object_map()`.  Records
    of a type registered with a `TypeObjectMap`, or a class, decode into
    objects in the `*_and_decode` methods, and `GPUdbTable.insert_records()`
    and `GPUdbIngestor` take objects of a class with a registered map, or of
    any dataclass whose fields are named as the columns.
-   `GPUdb.known_types`, the known types as a dict of type ID to type: the
    same dict as `get_known_types`, which stays the property it has been.
-   `get_records_by_key()`, on `RecordRetriever` and `GPUdbTable`, no longer
    requires key values: it takes key values, an expression, both, or neither
    to fetch the whole table; and an `offset`, to page through the result.
-   `get_columns_by_key()`, on `RecordRetriever` and `GPUdbTable`, fetches
    only the columns named, with the same arguments as
    `get_records_by_key()`.
-   `RecordRetriever` takes `options` that apply to every lookup it makes;
    options given to a lookup override them.
-   `named_rows` on `GPUdb.query()`, `query_one()` and `GPUdbSqlIterator`
    gives each row as a `GPUdbSqlRow`: the list of its values, also readable
    by column name (`row["city"]`, `get()`, `keys()`, `items()`,
    `as_dict()`).  Rows are plain lists by default, as before.
-   Query parameters as their JSON, in a string: an array of values, or of
    arrays of them to insert several rows with one statement; and as a tuple,
    in `to_df()` too.
-   `GPUdbFileHandler.ingest()` uploads local files, directories or glob
    patterns to a staging directory under the user's KIFS home, ingests them
    into a table, creating it if need be, and deletes the staged copies
    whether or not the ingest succeeded.  It returns an `IngestResult` and
    raises a `GPUdbException` on failure.  `IngestOptions` and
    `TableCreationOptions` give the ingest's options, with `FileType`,
    `ErrorHandlingMode`, `TypeInferenceMode` and `PartitionType` for their
    fixed values; any other option can be given with `put()`.
-   KIFS transfers of many files:
    -   `GPUdbFileHandler.upload_files()` takes directories (`recursive=True`
        for their subdirectories) and glob patterns (`**` for any depth)
        besides files, and uploads files under their paths from the directory
        or pattern.  It and `upload_file()` take a `ttl`, in minutes.
    -   `download_files()` takes KIFS directories and wildcard patterns
        besides files, and saves files under their paths from the directory
        or pattern; it and `download_file()` take `overwrite` (default True)
        to keep local files that exist.  `download_dir()` downloads a KIFS
        directory.
    -   Both refuse, before transferring anything, names that match no file,
        naming every one, and files that would be transferred as one.
    -   A KIFS pattern's wildcards are `*` and `?`; a KIFS name with `[` in it
        is a name.  A local name with `[` is a glob pattern unless a file of
        that very name exists.
    -   Many small files are sent in each request, both ways, and uploads
        send several requests at once.  `GPUdbFileHandler.Options`
        (`file_size_to_split`, 60 MB; `full_file_dispatcher_threadpool_size`,
        5; `max_upload_bytes_in_flight`, 128 MB) set the limits.
    -   A `callback` on every upload and download method receives a
        `TransferResult` for each file, and each part of a file sent in parts.
    -   `show_files()`, `show_directory()`, `show_directories()` and
        `show_all_directories()`, giving `KifsFileInfo` (with creator, creation
        time and info) and `KifsDirectoryInfo`; `create_directory()`,
        `delete_directory()`, `delete_files()` and `delete_files_in_dir()`; and
        `kifs_file_exists()`, `kifs_directory_exists()` and
        `kifs_directories_exist()`.  All take `~` for the user's home directory.
-   `gpudb.set_logging_level()` sets the log level of the whole API: every
    `GPUdb`, table, ingestor, record retriever, table monitor and fail-back
    poller, except an object given a level of its own.  It takes TRACE, DEBUG,
    INFO, WARN or WARNING, ERROR, CRITICAL or OFF, in any case, or a level
    number.  The `GPUDB_LOG_LEVEL` environment variable sets it at startup.

#### Changed
-   TCP keep-alive is on for connections to the database, so that a
    long-running request behind a NAT, a firewall or a load balancer is not
    dropped as idle; `tcp_keep_alive` turns it off.
-   Every request has `server_connection_timeout` (5 seconds by default) to
    connect, never longer than its `timeout`; 0 puts no limit.  A host that
    did not answer used to hold a request for the operating system's connect
    timeout at every retry, and `wms()`, `ping()`,
    `get_server_debug_information()` and the JSON requests without a body had
    no timeout at all.
-   A pooled connection idle for more than 100 seconds is opened afresh rather
    than reused, since a NAT or a firewall may have dropped it silently.
-   Connecting keeps the URL it was given as the cluster's URL, and
    `get_url()` returns it.  It used to be replaced by the address the server
    advertises for itself, which may not be reachable from the client, or may
    reach a different server.
-   A cluster that cannot be reached at any address the server advertises for
    it is used through the URL given for it, without multi-head I/O; the
    other clusters of the HA ring are unaffected, and are still found through
    it.  This used to turn off discovery, and so multi-head I/O and HA
    failover, for the whole connection.
-   A cluster's host manager address is taken from what that cluster
    publishes, so that host manager requests to a cluster on another port, or
    to one of several clusters behind httpd, no longer fail.
    `GPUdb.Options.host_manager_port` applies only when set to other than the
    default, 9300.
-   A JSON request (`insert_records_json()`, `get_records_json()`, ...)
    answered with an HTTP 400 saying the server is shutting down, or draining
    its HA queue, fails over as a binary request does, rather than failing.
-   `is_kinetica_running()` waits at most 5 seconds for an answer; it used to
    wait the client's `timeout`, none by default, so a server that took the
    connection but never answered hung the check.  Given `None`, it checks the
    current cluster without failing over; it used to refuse `None`.
-   `ping()` and `is_kinetica_running()` make one attempt; a host that was
    down used to take six timeouts to report.
-   An HTTP 403 response raises `GPUdbUnauthorizedAccessException`, as a 401
    does; the client used to fail with a decoding error on the error page.
-   The API's log messages go to the application's logging handlers: every
    logger of the API is under the `gpudb` logger and passes its records on.
    Until the application sets up a handler, the API still writes to stderr,
    in its own format, as before; once it has, on any logger of the API or
    above, nothing is written twice.
-   The fail-back poller logs under its `GPUdb`'s logger and follows that
    client's level; the table monitor logs under `gpudb.TableMonitor` at the
    API's level, so its INFO messages are written only when that level is set
    (`gpudb.set_logging_level("INFO")`, or the monitor's `logging_level`).
-   The `requests` and `urllib3` loggers, which log every connection and
    request at DEBUG, are set to WARNING once, when the API is imported; after
    that, the application can set its own log level on each, by name.
-   Multi-head ingest resends a batch that failed on a connection error to the
    same cluster up to 3 times (`GPUdbIngestor.retry_count`, which was 1)
    before failing over, so that a brief network problem no longer fails an
    ingest on a cluster with no HA ring.  A batch the server refuses, as for a
    bad record, fails at once rather than being resent to be refused again.
-   With the `return_individual_errors` insert option and partial batches off
    (`allow_partial_batch` not `true`, or `error_handling` `abort`), multi-head
    ingest stops taking records at the first rejected batch, so that none
    after it are inserted, and takes them again once `get_errors()` has
    returned the error.  It used to go on inserting the batches after the
    rejected one.
-   Inserts and updates are compressed with Snappy by default where
    `python-snappy` is installed (the `fast` extra); `use_snappy` turns it
    off.  No other request is compressed, nor one in JSON, nor one over
    1.7 GiB, which Snappy cannot compress (with a warning the first time).
    The `SNAPPY` encoding, which compresses every request, is unchanged.
-   `GPUdbIngestor` with `json_ingestion` sends JSON records to the worker
    ranks directly, as it does binary ones; they all went to the head node.
    A JSON record must now be a string, and is parsed to check it only with
    the `validate` JSON option; the server refuses a batch holding an invalid
    one.
-   `GPUdbIngestor` checks, when it is made, that the user may insert into the
    table, and update it with the `update_on_existing_pk` option, and finds
    out whether the table is replicated where `is_table_replicated` is not
    given (it now defaults to None; it was False).  It raises a
    `GPUdbException` if the user may not, or if the table does not exist,
    which used to surface at the first flush.  This costs two or three
    requests when the ingestor is made; `check_permissions=False` leaves the
    permission check out, and `is_table_replicated` the table lookup.  A
    `GPUdbTable`'s own ingestor does not check.
-   A `TypeObjectMap` registered for a type stays registered when the type is
    met again, as by `show_table()`; it used to be replaced by the plain type.
-   A client name given without a client version (`GPUdb.Options.client_name`)
    is sent in the User-Agent header; it used to be dropped silently.
-   `RecordRetriever` finds out whether its table is replicated where
    `is_table_replicated` is not given (default None), and looks a replicated
    table up on a worker rank, every page of one lookup from the same one; it
    used to use the head node, and only if told the table was replicated.
    The whole table, and a sorted lookup, still come from the head node.  A
    lookup retried after a failure fetches the same page.
-   With `convert_special_types_on_retrieval`, `GPUdbTable.get_records()`,
    `get_records_by_key()` and row-major `get_records_by_column()` return the
    records they convert as `GPUdbRecordDict`s: ordered dicts of column name
    to value that also take a column's position as an index and carry their
    `type`, as a `Record` does.  Iterating one yields the column names, as
    iterating any dict does, where iterating a `Record` yields the values.
    A `Record` cannot hold the list an array converts to, nor the dict a JSON
    value converts to; such values used to come back as Python's repr text,
    and vectors as text.
-   `GPUdbWorkerList`'s `ip_regex` is ignored, with a warning; use
    `GPUdb.Options.hostname_regex`, which applies to every worker list of the
    connection.  A server reporting multi-head enabled but advertising no
    worker addresses no longer fails building a worker list; the client warns
    and uses the head node alone.
-   `GPUdbFileHandler.upload_files()` and `download_files()` go on with the
    other files when one fails, then raise a `GPUdbException` naming every
    file that failed, also given as the error's `failures`, a dict of file
    name to error message; they used to stop at the first.  A single name can
    be given as a string.

#### Fixed
-   A request that failed on a cluster still reporting itself running and not
    draining, as with an HTTP 500 from a broken rank, being resent to that
    cluster forever instead of failing over.  Failover now moves on to the
    next usable cluster, and raises an error once every cluster has been
    tried.
-   Several threads failing over at the same time could skip a cluster, or
    adopt one another thread had switched to without checking it.  Failovers
    and fail-backs no longer interleave, and an adopted cluster is checked
    before use.
-   One fail-back poller being shared by every `GPUdb` in the process: only
    the first client to fail over was ever failed back, and closing it stopped
    fail-back for all.  Each `GPUdb` now has its own.
-   Multi-head ingest and key lookups going on using the backup cluster's
    workers after the client failed back to its primary.  Both now follow the
    client to whichever cluster it is on.
-   A failed host manager request (`admin_add_host`, `admin_alter_host`,
    `admin_remove_host`, `admin_show_alerts`, `admin_switchover`) being
    replayed on the next cluster in the HA ring, where it could act on a
    cluster the caller never named, and moving the database connection even
    when that database was healthy.  It is now sent only to the current
    cluster's host manager, and a failure is returned to the caller.
-   A server certificate that failed verification being taken for a dead
    server, so that the client failed over and the user saw an HA error such
    as "Only one cluster in ring" instead of the certificate error.  It is now
    raised at once as a `GPUdbUnauthorizedAccessException`, with a hint on
    adding the certificate to the trust store or using
    `skip_ssl_cert_verification`.
-   Setting `GPUdb.Options.hostname_regex` making `GPUdb()` fail with a
    `TypeError` whenever auto-discovery was on, which it is by default.
-   A hostname regex matching none of the addresses the server advertises
    failing the connection even when it matched a URL the user gave, as with
    a server behind a port mapping.  The connection now uses the user-given
    URLs the regex matches, without multi-head I/O, and fails only if it
    matches none of them.
-   A cluster being taken for another whose hostname starts with its own, as
    `10.0.0.1` for `10.0.0.12`, which dropped it from the HA ring while
    connecting and could resolve a primary URL to the wrong cluster.
-   A cluster reached at a remote address that advertises this machine's own
    address, such as `127.0.0.1`, for its ranks being connected to at that
    address, so that requests went to whatever was listening locally.  Such an
    address is never used where the server was reached elsewhere.  An
    advertised address naming no host (`http://:9191`) is skipped rather than
    taken for `127.0.0.1`.
-   With SNAPPY encoding, `ping()`, `is_kinetica_running()`,
    `get_server_debug_information()` and `wms()` failing before anything was
    sent, so that a running server was reported down and a client was never
    failed back to its primary.
-   `remove_http_header()` not stopping a header given in
    `GPUdb.Options.http_headers` from being sent, and JSON requests
    (`insert_records_from_json()`, `get_records_json()`) not sending headers
    added with `add_http_header()`, nor the header of a non-default
    `ha_sync_mode`.
-   A copy of `GPUdb.Options` (`GPUdb.Options( options )`) not equaling the
    original, an unset `primary_url` and `primary_host` becoming `""`; and
    `FailbackOptions` comparing by identity, so that the options of an
    unpickled `GPUdb` did not equal the original's.
-   The documentation of `GPUdb.Options.timeout`,
    `initial_connection_attempt_timeout` and `server_connection_timeout`
    giving them in milliseconds; all three are, and remain, in seconds.  A
    `timeout` of `None`, the default, means no timeout, where it said 0 did;
    0 makes every request fail.  The DB-API connection's message about
    `timeout` likewise said milliseconds.
-   Shard keys built differently from the database's for some values, so that
    distributed ingest performed poorly and distributed key lookup failed:
    -   UUIDs without hyphens, when they had a hexadecimal letter
    -   `charN` strings with non-ASCII characters, now padded and cut to N
        bytes instead of rejected
    -   decimals given as numbers, in exponent form, or with more decimal
        places than the scale (rounded half up)
    -   booleans given as strings, or as numbers other than 0 and 1 (a NaN is
        false and an infinity true)
-   The records an `InsertionException` from `GPUdbIngestor.insert_records()`
    returns repeating the one whose batch failed to flush, and others equal to
    it, so that re-sending them inserted duplicates.
-   An `InsertionException` not unpickling, as when passed back from a
    `multiprocessing` worker.  It now pickles with its records and URL; a
    record it holds as a `Record` goes as the dict of its values.
-   A process forked after connecting, as `multiprocessing` with the `fork`
    start method makes, sharing the connection's open sockets with the
    parent and its sibling processes, so that requests sent at the same time
    garbled one another; a batch of multi-head ingest from such a worker was
    refused as unreadable.  A forked process now opens connections of its
    own.
-   `GPUdbIngestor.flush()` and `insert_records()` with `json_ingestion`
    raising a `KeyError` once a second worker rank's queue was flushed, after
    its records were inserted.  They now return the JSON response with every
    queue's counts.
-   `GPUdbTable.insert_records()` writing arrays as text joined by commas, so
    that the server rejected any `array(string)` given as a list, and any
    array holding a `None`.  Arrays are now sent as JSON.
-   A `Decimal` being sent to a decimal column in exponent form for some
    values, such as `1E+3`, whether given in a record or as an attribute of
    an object that a `TypeObjectMap` maps.  It is now sent in plain notation.
-   `RecordRetriever.get_records_by_key()` and `GPUdbTable.get_records_by_key()`
    raising an `AttributeError` when given key values in a dict for a table
    with neither a shard key nor a primary key.  Key values are now refused for
    such a table whether listed or named, with a `GPUdbException`, as a list
    already was: the table has no key columns to associate them with.  Such a
    table is looked up by expression alone.
-   `get_records_by_key()` building a filter that some key values broke: a
    double quote in a string, a null given in a dict of key values (written
    as `"None"`), a boolean (written as `True`) and an unsigned long (quoted).
    The server rejected such a filter, could not route it, or matched
    nothing.
-   `get_records_by_key()` adding its key filter to the caller's options dict,
    which then carried it into the caller's next request.
-   With `convert_special_types_on_retrieval`, arrays being parsed with
    `eval()`, which failed on any array holding a null and ran whatever
    Python the text held; vectors being returned as text rather than a list
    of floats; and empty arrays and JSON objects being returned unconverted.
    A vector given to `insert_records()` as text was `eval()`'d too.  Both
    are now parsed as JSON.
-   Query parameters given as a string being split into characters, one
    parameter each, by `GPUdb.query()`, `query_one()`, `execute()`, `to_df()`
    and `GPUdbSqlIterator`; a string is now taken as the parameters' JSON.  A
    string that is not a JSON array, and a dict, set or other value that is
    not an ordered collection, are refused before the query is sent.
    `to_df()` refused a tuple.
-   `GPUdb.execute()` sending the query parameters of an earlier call again
    with a later call that gave none, and turning vectors in the caller's
    parameter list into strings in place.
-   A statement that returns no rows, such as an `INSERT`, being sent to the
    server twice by `GPUdb.query()` and `GPUdb.query_one()`, then failing with
    an `IndexError`; `query_one()` instead reported "More than one result was
    returned" when several rows were affected.  Such a statement now runs
    once, and returns no rows.
-   `GPUdbSqlIterator.close()` stopping at the first paging table it could not
    drop, leaving the rest on the server, and, when leaving a `with` block,
    replacing any exception raised inside it with that error.  Every table
    that can be dropped now is, and a failure is logged.
-   `GPUdbFileHandler` downloads that failed leaving a partial local file
    behind, a short or empty part of a large file being skipped silently, and
    an error from the server, as for a file not found, surfacing as a
    `KeyError`.  A failed download now raises a `GPUdbException` and deletes
    the local file.
-   `GPUdbFileHandler` multipart uploads sending an empty part after the last,
    and leaving the upload open on the server when a request raised or the
    local file could not be read.  The upload is now cancelled whenever it
    does not complete; a failure to cancel is logged.
-   `GPUdbFileHandler` not uploading a zero-byte file, without an error.
-   `GPUdbTableMonitor.Client()` hanging, at random, while checking that the
    server's table monitor answers.  It now waits at most `wait_interval`
    seconds (at least one) per try, gives up on an address that does not
    answer after `max_retries` tries, and no longer leaks a socket and a
    thread each time.
-   An undecodable server response during multi-head ingest or a key lookup
    surfacing as an `AttributeError` rather than a `GPUdbDecodingException`,
    and a `GPUdbException` wrapping an exception with no message, such as
    `TimeoutError()`, raising an `IndexError`.
-   Each `GPUdb`, `GPUdbTable`, `GPUdbIngestor`, `RecordRetriever` and table
    monitor object registering a logger with Python's `logging` module that
    was never released, so that a long-running application grew with every
    one.  Each still has its own logger, and its own level.

#### Performance
-   Multi-head ingest is faster.  Measured on 200,000 records, a flush takes
    2.5 times less time than before, and queuing the records 1.6 times less
    (3.1 times with the `fast` extra).
-   KIFS transfers of many small files are batched.  Measured on a local
    server, 2000 files of 10 KB upload in about 1 s (6.5 s before) and
    download in under 1 s (7.9 s before); 200 files of 1 MB upload in 1.3 s
    (2.1 s before).

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.3.14 - 2026-10-28

#### Added
-   Support for pandas 3.
-   Per-connection sessions for SQL `#name` temp tables.  Each `GPUdb` is now
    its own session, so two `GPUdb` objects for the same user no longer share
    one `#name`, and the temp tables a session creates are dropped when its
    `GPUdb` is closed with the new `GPUdb.close()` or by leaving its `with`
    block.  One that is never closed has them dropped when it is deleted, on a
    best-effort basis.  Closing a DB-API connection closes its `GPUdb`.
    Several `GPUdb` objects can share one session by giving its ID in the new
    `GPUdb.Options.session_id`; such a session is not dropped on close, and is
    left to the caller to drop.  A `GPUdb`'s session ID is available from the
    new `GPUdb.session_id`.


### Version 7.2.3.13 - 2026-09-30

#### Changed
-   Multi-head being turned off because cluster discovery failed is now
    reported as a warning; it was previously recorded only at debug level.
    Disabling auto-discovery deliberately is not a demotion and stays quiet.

#### Fixed
-   `GPUdbSqlIterator` and `DataFrameUtils.sql_to_df` sharing one server-side
    paging table when built from a single options mapping.  Used at the same
    time, the two raced to create it and one failed; where they did not overlap,
    either could return the other's rows instead of its own or fail with an
    index-out of-range error, and closing either dropped the table the other
    was still reading.  Both now copy the options mapping and parameter list
    they are given.  A result read in a single page was unaffected.
-   One malformed address among those a worker rank advertises degrading the
    whole connection to head-node only, or failing it outright when a hostname
    regex was in use.  The rank's other addresses are now tried, and the
    connection degrades only if none of them is usable.
-   Multi-head ingestion sending records to a rank that had been removed from
    the cluster, which failed with an `AttributeError`.  Ingestion and record
    retrieval now raise a `GPUdbException` naming the rank.
-   Multi-head ingestion and retrieval sending records to the first worker rank
    instead of the head node, when a worker list was supplied and the head node
    was the one to be used--a replicated table, or auto-discovery turned off.
-   A worker list built with `use_head_node_only` being routed as though it
    were a multi-head list, which failed a sharded insert or lookup with
    `Sharded worker index is out of bound`.  The request is now honored.
-   A record whose shard key held a negative zero or a NaN being sent to a
    different rank than the one the database assigns it.
-   Sharded insertion and lookup sending every record to the first worker
    rank when no shard mapping was available.  Records now go through the head
    node when the database cannot be reached, and the operation fails with an
    error naming the table when the database returns an empty mapping.

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.3.12 - 2026-08-20

#### Changed
-   Updated DBAPI `sqlparse` library to 0.6.x, which addresses CVE-2026-71491,
    CVE-2026-54284, CVE-2026-59893 & CVE-2026-59894.
-   Minimum supported Python version is now 3.10.

#### Fixed
-   Table monitor shutdown terminating its ZMQ context from two threads at
    once, which raised `ZMQError: Bad address` from the monitor thread or left
    that thread wedged inside `zmq_ctx_term()`.  `stop()` now only signals the
    monitor thread, which continues to own socket & context teardown; the
    receive loop polls in short slices so shutdown stays prompt.


### Version 7.2.3.11 - 2026-08-19

#### Added
-   Support for server-side default column values on insertion.  Records built
    from a type covering only some of a table's columns now have the rest
    populated by the server from their `default`, `init_with_now` or
    `init_with_uuid` values, rather than being rejected.  This is done by
    declaring the columns being sent via the *request_schema_str* insertion
    option, and needs a server of at least v7.2.3.18.
-   `GPUdbTable` argument *insert_type*, the type that records given to
    `insert_records()` are encoded with; any column of the table that it
    leaves out is populated by the server.  Records are still read back with
    the table's own type.  Also `GPUdbTable.get_insert_type()`.
-   `GPUdbIngestor` declares the columns it is sending, taken from the record
    type it was constructed with.
-   `GPUdbRecordType.base_definition`, the bare column definition that the
    *request_schema_str* option takes.

#### Changed
-   An unparseable UUID or out-of-range timestamp is no longer rejected while
    calculating a record's shard key; such a value in a record being inserted
    is now reported by the server instead.
-   `GPUdbIngestor` now keeps its own copy of the options it is given, so it
    neither writes the options it adds back into the caller's dictionary nor
    picks up changes made to that dictionary after construction.

#### Fixed
-   Multi-head ingestion into a sharded table whose shard key column supplies
    its own value via `init_with_now` or `init_with_uuid`; the placeholder that
    a record carries for such a column no longer raises an error.

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.3.10 - 2026-07-09

#### Added
-   Checking of draining HA queue status on connection; fail over if so.

#### Changed
-   Fail-over/fail-back processing to handle query planner down state.

#### Fixed
-   Bug in inferring a primary URL from user specifying only one connection
    cluster.

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.3.9 - 2026-06-22

#### Added
-   Passing of API/DBAPI name & version in HTTP headers.

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.3.8 - 2026-04-26

#### Added
-   Option to change the number of automatic retries of failed requests
-   Clean up of fast connection manager

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.3.7 - 2026-03-11

#### Added
-   Support for Python 3.14


### Version 7.2.3.6 - 2026-02-24

#### Changed
-   Reduced timeout for checking connectivity to server-known URLs that don't
    match user-given ones
-   Default HA fail-over mode is now sequential
-   Streamlined connection sequence & logging
-   Updated DBAPI `sqlparse` library to 0.5.4
-   Updated DBAPI examples

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes


### Version 7.2.3.5 - 2026-02-05

#### Added
-   Connection parameter `primary_url` to replace `primary_host`
-   Surfaced FailbackOptions class, accessible via gpudb.FailbackOptions
-   Fail-back check to ensure database query planner is up & running
-   New function to return primary cluster head node URL, for use by fail-back
    poller; previously, poller would construct the URL from the hostname

#### Changed
-   Aligned fail-back poller logging level with main connection's logging level
-   Changed retry mechanism to return error encountered instead of raising a new
    exception

#### Fixed
-   Fail-back poller now invoked using user-specified fail-back options


### Version 7.2.3.4 - 2026-01-28

#### Added
-   Support for full range of connection options when using the DBAPI


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
