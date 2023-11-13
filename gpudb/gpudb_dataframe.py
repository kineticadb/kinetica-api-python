# ---------------------------------------------------------------------------
# gpudb_dataframe.py - Kinetica API interaction with pandas dataframes.
#
# Copyright (c) 2023 Kinetica DB Inc.
# ---------------------------------------------------------------------------

import logging
import pandas as pd
import numpy as np
try:
    from math import ceil, log
except:
    from math import ceil, log2

import json
from tqdm.auto import tqdm


from . import GPUdbRecordColumn
from . import GPUdbColumnProperty
from . import GPUdbTable
from . import GPUdbSqlIterator
from . import GPUdbException


class DataFrameUtils:
    _COL_TYPE = GPUdbRecordColumn._ColumnType
    _LOG = logging.getLogger("gpudb.DataFrameUtils")
    TQDM_NCOLS = None
    BATCH_SIZE = 5000

    @classmethod
    def vec_to_bytes(cls, vec):
        return np.array(vec).astype(np.float32).tobytes()

    @classmethod
    def bytes_to_vec(cls, bvec):
        return np.frombuffer(bvec, dtype=np.float32)

    @classmethod
    def sql_to_df(cls, db,
                  sql,
                  param_list= None,
                  batch_size = BATCH_SIZE,
                  sql_opts=None,
                  show_progress = False):
        """Create a dataframe from the results of a SQL query.

        Args:
            db (GPUdb): a GPUdb instance
            sql (str): the SQL query
            param_list (list): the query parameters. Defaults to None.
            batch_size (int): the batch size for the SQL execution results. Defaults to BATCH_SIZE.
            sql_opts (dict): the SQL options as a dict. Defaults to None.
            show_progress (bool): whether to display progress or not. Defaults to False.

        Raises:
            GPUdbException: 

        Returns:
            pd.DataFrame: a Pandas dataframe or None if the SQL has returned no results
        """        

        if sql_opts is None:
            sql_opts = {}
        if (param_list is not None):
            for idx, item in enumerate(param_list):
                if (isinstance(item, list)):
                    # assume that list type is vector
                    param_list[idx] = str(item)
            json_list = json.dumps(param_list)
            sql_opts['query_parameters'] = json_list

        cls._LOG.debug('Getting records from <{}>'.format(sql))

        if (show_progress):
            print("Executing SQL...")

        result_list = []
        with GPUdbSqlIterator(db, sql,
                              batch_size=batch_size,
                              sql_opts=sql_opts) as sql_iter:
            if (sql_iter.type_map is None):
                # If there are no results then we can't infer datatypes.
                cls._LOG.debug("SQL returned no results.")
                return None
            
            for rec in tqdm(iterable=sql_iter,
                            total=sql_iter.total_count,
                            desc='Fetching Records',
                            disable=(not show_progress),
                            ncols=cls.TQDM_NCOLS):
                result_list.append(rec)


        result_df = cls._convert_records_to_df(result_list, sql_iter.type_map)

        if (sql_iter.total_count != result_df.shape[0]):
            raise GPUdbException(
                "Incorrect record count: expected={} retrieved={}".format(sql_iter.total_count, result_df.shape[0]))
        return result_df

    TYPE_GPUDB_TO_NUMPY = {
        _COL_TYPE.LONG: 'int64',
        _COL_TYPE.INT: 'int32',
        GPUdbColumnProperty.INT16: 'int16',
        GPUdbColumnProperty.INT8: 'int8',
        _COL_TYPE.DOUBLE: 'float64',
        _COL_TYPE.FLOAT: 'float32',
        GPUdbColumnProperty.DATETIME: 'string'
    }

    @classmethod
    def _convert_records_to_df(cls, records, type_map):
        """Create a Pandas dataframe from a list of records

        Args:
            records (list): the list of records
            type_map (dict): the column type mapping

        Returns:
            pd.Dataframe: a Pandas dataframe
        """        
        col_major_recs = zip(*records)
        data_list = []
        index = pd.RangeIndex(0, len(records))

        # convert each column individually to avoid un-necessary conversions
        for col_name, raw_data in zip(type_map.keys(), col_major_recs):
            gpudb_type = type_map[col_name]
            numpy_type = cls.TYPE_GPUDB_TO_NUMPY.get(gpudb_type)
            col_data = pd.Series(data=raw_data,
                                 name=col_name,
                                 dtype=numpy_type,
                                 index=index)

            # do special conversion
            if (gpudb_type == cls._COL_TYPE.BYTES):
                col_data = col_data.map(cls.bytes_to_vec)
            elif (gpudb_type == GPUdbColumnProperty.TIMESTAMP):
                col_data = pd.to_datetime(col_data, unit='ms')
            elif (gpudb_type == GPUdbColumnProperty.DATETIME):
                col_data = pd.to_datetime(col_data, format='%Y-%m-%d %H:%M:%S.%f')

            data_list.append(col_data)
        return pd.concat(data_list, axis=1)

    @classmethod
    def table_to_df(cls, db,
                    table_name,
                    batch_size = BATCH_SIZE,
                    show_progress = False):
        """Convert a Kinetica table into a dataframe and load data into it. 

        Args:
            db (GPUdb): a GPUdb instance
            table_name (str): name of the Kinetica table
            batch_size (int): the batch size for the SQL execution results. Defaults to BATCH_SIZE.
            show_progress (bool): whether to display progress or not. Defaults to False.

        Returns:
            pd.Dataframe: Returns a Pandas dataframe created from the Kinetica table
        """

        sql = "SELECT * FROM {}".format(table_name)
        return cls.sql_to_df(db=db,
                             sql=sql,
                             batch_size=batch_size,
                             show_progress=show_progress)

    @classmethod
    def table_type_as_df(cls, gpudb_table):
        """Convert a GPUdbTable's type schema (column list) into a dataframe. 

        Args:
            gpudb_table (GPUdbTable): a GPUdbTable instance

        Returns:
            pd.DataFrame: a Pandas dataframe created by analyzing the table column types
        """        

        table_type = gpudb_table.get_table_type()
        col_list = []
        for col in table_type.columns:
            col_type = [col.name, col.column_type, col.column_properties]
            col_list.append(col_type)
        return pd.DataFrame(col_list, columns=['name', 'type', 'properties'])

    @classmethod
    def df_to_table(cls, df,
                    db,
                    table_name,
                    column_types = None,
                    clear_table = False,
                    create_table = True,
                    load_data = True,
                    show_progress = False,
                    batch_size = BATCH_SIZE,
                    **kwargs):
        """ Load a dataframe into a table; optionally dropping any existing table,
        creating it if it doesn't exist, and loading data into it; and then returning a
        GPUdbTable reference to the table.


        Args:
            db (GPUdb): a GPUdb instance
            table_name (str): the Kinetica table name
            column_types (dict): Kinetica column specs. Defaults to None.
            clear_table (bool): whether to clear records from the existing table or not. Defaults to False.
            create_table (bool): whether to create a non-existing table or not. Defaults to True.
            load_data (bool): whether to load data into the table or not. Defaults to True.
            show_progress (bool): whether to show progress of the operation. Defaults to False.
            batch_size (int): a batch size to use for loading data into the table. Defaults to BATCH_SIZE.

        Raises:
            GPUdbException: 

        Returns:
            GPUdbTable: a GPUdbTable instance created from the dataframe passed in 
        """        

        has_table_resp = db.has_table(table_name)
        cls._check_error(has_table_resp)
        if (not create_table and not has_table_resp["table_exists"]):
            raise GPUdbException("({}) Table does not exist and create_table=false".format(table_name))

        if (clear_table):
            cls._LOG.debug("Clearing table: {}".format(table_name))
            clear_resp = db.clear_table(table_name=table_name, options={'no_error_if_not_exists': 'true'})
            cls._check_error(clear_resp)

        cls._LOG.debug("Creating table: {}".format(table_name))
        col_types = cls._table_types_from_df(df, column_types)
        gpudb_table = GPUdbTable(_type=col_types, name=table_name, db=db, **kwargs)

        if (load_data):
            cls.df_insert_into_table(df=df,
                                     gpudb_table=gpudb_table,
                                     show_progress=show_progress,
                                     batch_size=batch_size)
        return gpudb_table

    @classmethod
    def _check_error(cls, response):
        status = response['status_info']['status']
        if (status != 'OK'):
            message = response['status_info']['message']
            raise GPUdbException('[%s]: %s' % (status, message))

    @classmethod
    def df_insert_into_table(cls, df,
                             gpudb_table,
                             batch_size = BATCH_SIZE,
                             show_progress = False):
        """Load a dataframe into a GPUdbTable. 

        Args:
            df (pd.Dataframe): a Pandas dataframe
            gpudb_table (GPUdbTable): a GPUdbTable instance
            batch_size (int): a batch size to use for loading data into the table. Defaults to BATCH_SIZE.
            show_progress (bool): whether to show progress of the operation. Defaults to False.

        Returns:
            int: the number of rows of the dataframe actually inserted into the Kinetica table
        """        

        total_rows = df.shape[0]
        rows_before = gpudb_table.size()
        converted_df = cls._table_convert_df_for_insert(df)

        cls._LOG.debug("Inserting rows into <{}>".format(gpudb_table.table_name))
        with tqdm(total=total_rows,
                  desc='Inserting Records',
                  ncols=cls.TQDM_NCOLS,
                  disable=(not show_progress)) as progress_bar:
            for _offset in range(0, total_rows, batch_size):
                end = min(total_rows, _offset + batch_size)
                slice = converted_df.iloc[_offset:end]

                # Convert to records so we can preserve the column dtypes
                insert_records = slice.to_records(index=False, column_dtypes=None)

                # Call item() so the types are converted to python native types
                insert_rows = [list(x.item()) for x in insert_records]

                gpudb_table.insert_records(insert_rows)
                progress_bar.update(len(insert_rows))

        rows_inserted = gpudb_table.size() - rows_before
        cls._LOG.debug("Rows inserted: {}".format(rows_inserted))
        return rows_inserted

    @classmethod
    def _table_convert_df_for_insert(cls, df):
        """ Convert dataframe for insert into Kinetica table. """
        data_list = []

        for col_name, col_data in df.items():
            ref_val = col_data[0]
            if isinstance(ref_val, pd.Timestamp):
                col_data = col_data.view(np.int64) // int(1e6)
            elif isinstance(ref_val, list) or isinstance(ref_val, np.ndarray):
                col_data = col_data.map(cls.vec_to_bytes)
            data_list.append(col_data)

        return pd.concat(data_list, axis=1)

    TYPE_NUMPY_TO_GPUDB = {
        'int64': [_COL_TYPE.LONG],
        'int32': [_COL_TYPE.INT],
        'int16': [_COL_TYPE.INT, GPUdbColumnProperty.INT16],
        'int8': [_COL_TYPE.INT, GPUdbColumnProperty.INT8],
        'float64': [_COL_TYPE.DOUBLE],
        'float32': [_COL_TYPE.FLOAT],
        'datetime64[ns]': [_COL_TYPE.LONG, GPUdbColumnProperty.TIMESTAMP],
        'uint64': [_COL_TYPE.STRING, GPUdbColumnProperty.ULONG],
        'bool': [_COL_TYPE.INT, GPUdbColumnProperty.BOOLEAN]
    }

    @classmethod
    def _table_types_from_df(cls, df,
                             col_type_override = None):
        """ Create GPUdb column types from a DataFrame. """
        type_list = []

        # create a copy because we will be modifying this.
        col_type_override = col_type_override.copy()

        for col_name, col_data in df.items():
            np_type = col_data.dtype.name
            col_type = cls.TYPE_NUMPY_TO_GPUDB.get(np_type)

            if (col_type is None):
                # need to inspect the type directly
                ref_val = col_data[0]
                if isinstance(ref_val, str):
                    max_len = col_data.map(len).max()
                    spow = None
                    try:
                        spow = 2 ** ceil(log(max_len,2))
                    except:
                        spow = 2 ** ceil(log2(max_len))

                    col_type = [cls._COL_TYPE.STRING, 'char{}'.format(spow)]

                elif isinstance(ref_val, list) or isinstance(ref_val, np.ndarray):
                    vec_dim = len(ref_val)
                    col_type = [cls._COL_TYPE.BYTES, 'vector({})'.format(vec_dim)]

                else:
                    raise GPUdbException("{}: Type not supported: {}".format(col_name, type(ref_val)))

            col_attr_override = None
            if (col_type_override is not None):
                col_attr_override = col_type_override.get(col_name)

            if (col_attr_override is not None):
                # replace the column attribute if provided
                col_type = [col_type[0], col_attr_override]

            type_def = [col_name] + col_type
            type_list.append(type_def)
        return type_list

# end class DataFrameUtils
