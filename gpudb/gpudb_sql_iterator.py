# ---------------------------------------------------------------------------
# gpudb_sql_iterator.py - Kinetica API class for iterating over records
# returned by executing an SQL query.
#
# Copyright (c) 2023 Kinetica DB Inc.
# ---------------------------------------------------------------------------

import logging
import json

from . import GPUdb
from . import GPUdbTable
from . import GPUdbException

LOG = logging.getLogger(__name__)


class GPUdbSqlIterator():
    """  Iterates over the records of a given query.
    
    
    Example
    ::
    
        result_list = []
        with GPUdbSqlIterator(db, sql,
                              batch_size=batch_size,
                              sql_opts=sql_opts) as sql_iter:
            
            for rec in tqdm(iterable=sql_iter,
                            total=sql_iter.total_count,
                            desc='Fetching Records',
                            disable=(not show_progress),
                            ncols=cls.TQDM_NCOLS):
                result_list.append(rec)

    """

    _log = logging.getLogger(f"gpudb.GPUdbSqlIterator")

    def __init__(self,
                 db: GPUdb,
                 sql: str,
                 batch_size: int = 5000,
                 sql_params=[],
                 sql_opts: dict = {}):

        self.sql_params = sql_params
        self.sql = sql
        self.db = db
        self.batch_size = batch_size
        self.sql_opts = sql_opts

        # member vars
        self.type_map = None
        self.records = None
        self.offset = 0
        self.total_count = None
        self.retrieved_count = 0
        self.paging_tables = None

        paging_table_name = GPUdbTable.random_name()
        GPUdb._set_sql_params(sql_opts, sql_params)
        self.sql_opts["paging_table"] = paging_table_name

    def open(self):
        # optional call
        self._check_fetch()

    def close(self):
        if self.paging_tables:
            for table_name in self.paging_tables:
                self.db.clear_table(table_name, options={'no_error_if_not_exists': 'true'})

    def reset(self,
              sql: str,
              batch_size: int = 5000,
              sql_params=[],
              sql_opts: dict = {}
              ):
        self.sql = sql
        self.batch_size = batch_size
        self.sql_params = sql_params
        self.sql_opts = sql_opts
        # member vars
        self.type_map = None
        self.records = None
        self.offset = 0
        self.total_count = None
        self.retrieved_count = 0
        self.paging_tables = None

        paging_table_name = GPUdbTable.random_name()
        GPUdb._set_sql_params(sql_opts, sql_params)
        self.sql_opts["paging_table"] = paging_table_name

    def __enter__(self):
        # Called when entering a with clause
        self._log.debug("Enter iterator")
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Called when exiting a with clause
        self._log.debug("Exit iterator")
        self.close()

    def __iter__(self):
        # Called when starting an iterator
        self._log.debug("Start iterator")
        self.open()
        return self

    def __next__(self) -> list:
        self._check_fetch()
        if (self.records is None):
            raise StopIteration

        rec_values = self.records[self.rec_pos].values()
        self.rec_pos += 1
        self.retrieved_count += 1
        return rec_values

    def _check_fetch(self):
        if (self.records is not None and self.rec_pos < len(self.records)):
            # nothing to do
            return

        self.records = None
        self.rec_pos = 0

        if (self.total_count is not None and self.offset >= self.total_count):
            # no more records
            return

        self._execute_sql()
        self.offset += self.batch_size

    def _execute_sql(self):
        limit = self.batch_size
        if (self.total_count is not None):
            recs_remaining = self.total_count - self.offset
            limit = min(recs_remaining, self.batch_size)

        self._log.debug(f"SQL fetch: offset={self.offset} limit={limit}")
        response = self.db.execute_sql_and_decode(
            statement=self.sql,
            offset=self.offset,
            limit=limit,
            force_primitive_return_types=False,
            get_column_major=False,
            options=self.sql_opts)

        GPUdb._check_error(response)
        self.records = response['records']

        if (self.total_count is None):
            self.total_count = response['total_number_of_records'] if self.records else response['count_affected']

        if self.records and len(self.records) > 0 and self.paging_tables is None:
            paging_table_name = response.get("paging_table")
            if (paging_table_name):
                self.paging_tables = []
                self.paging_tables.append(paging_table_name)

            supporting_paging_tables = response["info"].get("result_table_list")
            if (supporting_paging_tables):
                self.paging_tables.extend(supporting_paging_tables.split(','))

            if (self.paging_tables is not None and len(self.paging_tables) > 0):
                self._log.debug(f"Paging tables: {self.paging_tables}")

        if (self.total_count == 0):
            return

        if self.type_map is None and self.records and len(self.records) > 0:
            col_defs = self.records[0].type.values()
            col_names = list(col.name for col in col_defs)
            col_types = list(col.data_type for col in col_defs)
            self.type_map = {name: type for (name, type) in zip(col_names, col_types)}
            self._log.debug(f"Type map: {self.type_map}")

    def __execute(self, sql: str, parameters=None):
        self.sql = sql
        self.sql_opts["query_parameters"] = parameters
        self.open()
        self.close()
        return self

# end class GPUdbSqlIterator
