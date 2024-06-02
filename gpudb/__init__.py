import sys

# C-extension package for avro encoding
try:                   # Installed
    from .protocol import RecordType
    from .protocol import Record
    from .protocol import Schema
except ImportError:    # Local
    from protocol import RecordType
    from protocol import Record
    from protocol import Schema


if (sys.version_info[0] == 3):  # checking the major component
    from gpudb.gpudb import GPUdb
    from gpudb.gpudb import GPUdbException
    from gpudb.gpudb import GPUdbConnectionException
    from gpudb.gpudb import GPUdbDecodingException
    from gpudb.gpudb import GPUdbExitException
    from gpudb.gpudb import GPUdbFailoverDisabledException
    from gpudb.gpudb import GPUdbHAUnavailableException
    from gpudb.gpudb import GPUdbHostnameRegexFailureException
    from gpudb.gpudb import GPUdbUnauthorizedAccessException
    from gpudb.gpudb import GPUdbRecordColumn
    from gpudb.gpudb import GPUdbRecordType
    from gpudb.gpudb import GPUdbRecord
    from gpudb.gpudb import GPUdbColumnProperty
    from gpudb.gpudb import GPUdbTable
    from gpudb.gpudb import GPUdbTableIterator
    from gpudb.gpudb import GPUdbTableOptions

    from gpudb.gpudb import AttrDict
    from gpudb.gpudb import _Util

    from gpudb.gpudb_multihead_io import GPUdbWorkerList, GPUdbIngestor, InsertionException, RecordRetriever

    from gpudb.gpudb_table_monitor import GPUdbTableMonitor
    from gpudb.gpudb_file_handler import GPUdbFileHandler, OpMode, MultipartOperation

    from gpudb.gpudb import collections
    from gpudb.gpudb_sql_iterator import GPUdbSqlIterator
    from gpudb.gpudb_sql_context import GPUdbSamplesClause, GPUdbTableClause, GPUdbSqlContext
    from gpudb.dbapi import *

else:
    from gpudb import GPUdb
    from gpudb import GPUdbException
    from gpudb import GPUdbConnectionException
    from gpudb import GPUdbDecodingException
    from gpudb import GPUdbExitException
    from gpudb import GPUdbFailoverDisabledException
    from gpudb import GPUdbHAUnavailableException
    from gpudb import GPUdbHostnameRegexFailureException
    from gpudb import GPUdbUnauthorizedAccessException
    from gpudb import GPUdbRecordColumn
    from gpudb import GPUdbRecordType
    from gpudb import GPUdbRecord
    from gpudb import GPUdbColumnProperty
    from gpudb import GPUdbTable
    from gpudb import GPUdbTableIterator
    from gpudb import GPUdbTableOptions

    from gpudb import AttrDict
    from gpudb import _Util

    from gpudb_multihead_io import GPUdbWorkerList, GPUdbIngestor, InsertionException, RecordRetriever

    from gpudb_table_monitor import GPUdbTableMonitor

    from gpudb import collections
    from gpudb_sql_iterator import GPUdbSqlIterator
    from gpudb_sql_context import GPUdbSamplesClause, GPUdbTableClause, GPUdbSqlContext
