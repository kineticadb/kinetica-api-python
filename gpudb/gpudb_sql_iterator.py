# ---------------------------------------------------------------------------
# gpudb_sql_iterator.py - Kinetica API class for iterating over records
# returned by executing an SQL query.
#
# Copyright (c) 2023 Kinetica DB Inc.
# ---------------------------------------------------------------------------

import collections
import logging
import json
from typing import Optional, Union

from . import GPUdb
from . import GPUdbTable
from . import GPUdbException

LOG = logging.getLogger(__name__)


class GPUdbSqlRow(list):
    """A row of a query's results, as :class:`GPUdbSqlIterator` gives it with
    ``named_rows``: the list of the row's values, in column order, whose
    values can also be read by column name.  Being a list, it unpacks,
    slices, compares and converts to
    JSON as the plain rows do.

    Example
    ::

        for row in db.query("SELECT id, name FROM people", named_rows=True):
            print(row["name"], row[0], row.as_dict())
    """

    __slots__ = ("_columns",)

    def __init__(self, values=(), columns=None):
        """The row's values, and each column's position by name (a dict
        shared by the rows of a query)."""
        super().__init__(values)
        self._columns = {} if columns is None else columns

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                key = self._columns[key]
            except KeyError:
                raise KeyError(key) from None
        return list.__getitem__(self, key)

    def get(self, name, default=None):
        """The value of a column by name, or the default if there is none."""
        position = self._columns.get(name)
        return default if position is None else list.__getitem__(self, position)

    def keys(self):
        """The column names, in order."""
        return list(self._columns)

    def items(self):
        """The (column name, value) pairs, in column order."""
        return list(zip(self._columns, self))

    def as_dict(self) -> collections.OrderedDict:
        """The row as an OrderedDict of its values by column name."""
        return collections.OrderedDict(zip(self._columns, self))

    def __repr__(self):
        return "GPUdbSqlRow({})".format(
                ", ".join("{}={!r}".format(name, value) for name, value in zip(self._columns, self)))

    def __reduce__(self):
        return (GPUdbSqlRow, (list(self), self._columns))

# end class GPUdbSqlRow


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
                 sql_params: Optional[Union[list, tuple, str]] = None,
                 sql_opts: Optional[dict] = None,
                 named_rows: bool = False):
        """
        Parameters:
            sql_params (list or tuple of native types, or str)
                The SQL parameters, or their JSON array as a string, as
                :meth:`GPUdb.query` takes them.
            named_rows (bool)
                Whether the rows are :class:`GPUdbSqlRow` lists, whose values
                can also be read by column name, rather than plain lists.
                Defaults to False.
        """
        self.sql_params = self.__copy_params(sql_params)
        self.sql_opts = dict(sql_opts) if sql_opts else {}
        self.sql = sql
        self.db = db
        self.batch_size = batch_size
        self.named_rows = named_rows

        # Each column's position, by name, for the named rows
        self._columns = None

        # member vars
        self.type_map = None
        self.records = None
        self.offset = 0
        self.total_count = None
        self.retrieved_count = 0
        self.paging_tables = []
        self.end_of_results = False

        paging_table_name = GPUdbTable.random_name()
        GPUdb._set_sql_params(self.sql_opts, self.sql_params)
        self.sql_opts["paging_table"] = paging_table_name

    def open(self):
        # optional call
        self._check_fetch()

    def close(self):
        # Drop every table, logging a failure rather than raising it.
        # Stopping at the first failure left the rest on the server; and
        # since __exit__ calls this, a raised failure replaced any exception
        # raised inside the `with` block.
        for table_name in self.paging_tables:
            try:
                self.db.clear_table(table_name, options={'no_error_if_not_exists': 'true'})
            except Exception as ex:
                self._log.warning(f"Could not drop paging table {table_name}: {ex}")

    def reset(self,
              sql: str,
              batch_size: int = 5000,
              sql_params: Optional[list] = None,
              sql_opts: Optional[dict] = None
              ):
        self.sql = sql
        self.batch_size = batch_size
        self.sql_params = self.__copy_params(sql_params)
        self.sql_opts = dict(sql_opts) if sql_opts else {}
        # member vars
        self._columns = None
        self.type_map = None
        self.records = None
        self.offset = 0
        self.total_count = None
        self.retrieved_count = 0
        self.paging_tables = []
        self.end_of_results = False

        paging_table_name = GPUdbTable.random_name()
        GPUdb._set_sql_params(self.sql_opts, self.sql_params)
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
        if self.named_rows:
            rec_values = GPUdbSqlRow(rec_values, self._columns)
        self.rec_pos += 1
        self.retrieved_count += 1
        return rec_values

    def _check_fetch(self):
        if (self.records is not None and self.rec_pos < len(self.records)):
            # nothing to do
            return

        self.records = None
        self.rec_pos = 0

        if (self.end_of_results
                or (self.total_count is not None and self.offset >= self.total_count)):
            # no more records
            return

        self._execute_sql()

        if not self.records:
            # A page with no records ends the results.  For a
            # statement that returns no rows (an INSERT, say), total_count is
            # the number of rows affected, so the offset never reaches it; the
            # statement must not be run again, nor its empty page read as rows.
            self.records = None
            self.end_of_results = True
            return

        # Account for max_get_records_size limiting server result set
        self.offset += len(self.records)

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

        if self.records and len(self.records) > 0 and not self.paging_tables:
            paging_table_name = response.get("paging_table")
            if (paging_table_name):
                # Store paging table name, if found, to clear later, when done
                self.paging_tables.append(paging_table_name)

                # Only clear result table list if a paging table was found
                supporting_paging_tables = response["info"].get("result_table_list")
                if (supporting_paging_tables):
                    self.paging_tables.extend(supporting_paging_tables.split(','))

                if (self.paging_tables):
                    self._log.debug(f"Paging tables: {self.paging_tables}")

        if (self.total_count == 0):
            return

        if self.type_map is None and self.records and len(self.records) > 0:
            col_defs = self.records[0].type.values()
            col_names = list(col.name for col in col_defs)
            col_types = list(col.data_type for col in col_defs)
            self.type_map = {name: type for (name, type) in zip(col_names, col_types)}
            self._columns = {name: position for (position, name) in enumerate(col_names)}
            self._log.debug(f"Type map: {self.type_map}")

    @staticmethod
    def __copy_params(sql_params):
        """The parameters, a list or tuple copied, so that the caller's may
        change; a string of their JSON is kept whole (list() split it into
        its characters).  GPUdb._set_sql_params() checks them.
        """
        if isinstance(sql_params, (list, tuple)):
            return list(sql_params)
        return [] if sql_params is None else sql_params

    def __execute(self, sql: str, parameters=None):
        self.sql = sql
        self.sql_opts["query_parameters"] = parameters
        self.open()
        self.close()
        return self

# end class GPUdbSqlIterator
