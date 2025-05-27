# ---------------------------------------------------------------------------
# gpudb_dataframe.py - Kinetica API interaction with pandas dataframes.
#
# Copyright (c) 2023 Kinetica DB Inc.
# ---------------------------------------------------------------------------

import logging
import pandas as pd
import numpy as np
from math import log2, ceil
import json
from tqdm.auto import tqdm

from typeguard import typechecked
from typing import List, Optional, Union

from . import GPUdbRecordColumn
from . import GPUdbColumnProperty
from . import GPUdb
from . import GPUdbTable
from . import GPUdbSqlIterator
from . import GPUdbException


class DataFrameUtils:

    _COL_TYPE = GPUdbRecordColumn._ColumnType
    _LOG = logging.getLogger(f"gpudb.DataFrameUtils")
    TQDM_NCOLS = None
    BATCH_SIZE = 5000

    @classmethod
    def vec_to_bytes(cls, vec: list) -> bytes:
        if vec is None:
            return None
        return np.array(vec).astype(np.float32).tobytes()

    @classmethod
    def bytes_to_vec(cls, bvec: bytes) -> np.ndarray:
        return None if not bvec else np.frombuffer(bvec, dtype=np.float32)


    @classmethod
    @typechecked
    def sql_to_df(cls, db: GPUdb,
                  sql: str,
                  sql_params: list = [],
                  batch_size: int = BATCH_SIZE,
                  sql_opts: dict = {},
                  show_progress: bool = False) -> Optional[pd.DataFrame]:
        """Create a dataframe from the results of a SQL query.

        Args:
            db (GPUdb): a GPUdb instance
            sql (str): the SQL query
            sql_params (list): the query parameters. Defaults to None.
            batch_size (int): the batch size for the SQL execution results. Defaults to BATCH_SIZE.
            sql_opts (dict): the SQL options as a dict. Defaults to None.
            show_progress (bool): whether to display progress or not. Defaults to False.

        Raises:
            GPUdbException: 

        Returns:
            pd.DataFrame: a Pandas dataframe or None if the SQL has returned no results
        """

        cls._LOG.debug('Getting records from <{}>'.format(sql))

        if(show_progress):
            print(f"Executing SQL...")

        result_list = []
        with GPUdbSqlIterator(db, sql,
                              batch_size=batch_size,
                              sql_params=sql_params,
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

        # if (sql_iter.total_count != result_df.shape[0]):
        #     raise GPUdbException(f"Incorrect record count: expected={sql_iter.total_count} retrieved={result_df.shape[0]}")
        return result_df


    TYPE_GPUDB_TO_NUMPY = {
        _COL_TYPE.LONG: 'Int64',
        _COL_TYPE.INT: 'Int64',
        GPUdbColumnProperty.INT16: 'Int16',
        GPUdbColumnProperty.INT8: 'Int8',
        _COL_TYPE.DOUBLE: 'Float64',
        _COL_TYPE.FLOAT: 'Float32',
        GPUdbColumnProperty.DATETIME: 'string'
    }

    @classmethod
    @typechecked
    def _convert_records_to_df(cls, records: list, type_map: dict) -> pd.DataFrame:
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
            try:
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

            except Exception as ex:
                msg = "Error converting column <{}> with data type <{}/{}>: {}".format(col_name, gpudb_type, numpy_type, ex)
                raise GPUdbException(msg) from ex

            data_list.append(col_data)
        return pd.concat(data_list, axis=1)


    @classmethod
    @typechecked
    def table_to_df(cls, db: GPUdb,
                    table_name: str,
                    batch_size: int = BATCH_SIZE,
                    show_progress: bool = False) -> pd.DataFrame:
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
    @typechecked
    def table_type_as_df(cls, gpudb_table: GPUdbTable) -> pd.DataFrame:
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
    @typechecked
    def df_to_table(cls, df: pd.DataFrame,
                    db: GPUdb,
                    table_name: str,
                    column_types: dict = {},
                    clear_table: bool = False,
                    create_table: bool = True,
                    load_data: bool = True,
                    show_progress: bool = False,
                    batch_size: int = BATCH_SIZE,
                    **kwargs) -> GPUdbTable:
        """ Load a Data Frame into a table; optionally dropping any existing table,
        creating it if it doesn't exist, and loading data into it; and then returning a
        GPUdbTable reference to the table.


        Args:
            df (pd.DataFrame)
                The Pandas Data Frame to load into a table

            db (GPUdb)
                GPUdb instance

            table_name (str)
                Name of the target Kinetica table for the Data Frame loading

            column_types (dict)
                Optional Kinetica column properties to apply to the column type definitions inferred
                from the Data Frame; map of column name to a list of column properties for that
                column, excluding the inferred base type. Defaults to empty map. For example::
                
                    { "middle_name": [ 'char64', 'nullable' ], "state": [ 'char2', 'dict' ] }

            clear_table (bool)
                Whether to drop an existing table of the same name or not before creating this one.
                Defaults to False.

            create_table (bool)
                Whether to create the table if it doesn't exist or not. Defaults to True.

            load_data (bool)
                Whether to load data into the target table or not. Defaults to True.

            show_progress (bool)
                Whether to show progress of the operation on the console. Defaults to False.

            batch_size (int)
                The number of records at a time to load into the target table. Defaults to BATCH_SIZE.

        Raises:
            GPUdbException: 

        Returns:
            GPUdbTable: a GPUdbTable instance created from the Data Frame passed in 
        """
        
        if(df.empty):
            raise GPUdbException(f"Dataframe cannot be empty.")

        has_table_resp = db.has_table(table_name)
        GPUdb._check_error(has_table_resp)
        if (not create_table and not has_table_resp["table_exists"]):
            raise GPUdbException(f"{table_name}) Table does not exist and create_table=false")

        if (clear_table):
            cls._LOG.debug(f"Clearing table: {table_name}")
            clear_resp = db.clear_table(table_name=table_name, options={'no_error_if_not_exists': 'true'})
            GPUdb._check_error(clear_resp)

        cls._LOG.debug(f"Creating table: {table_name}")
        col_types = cls._table_types_from_df(df, column_types)
        gpudb_table = GPUdbTable(_type=col_types, name=table_name, db=db, **kwargs)

        if (load_data):
            cls.df_insert_into_table(df=df,
                                     gpudb_table=gpudb_table,
                                     show_progress=show_progress,
                                     batch_size=batch_size)
        return gpudb_table


    @classmethod
    @typechecked
    def df_insert_into_table(cls, df: pd.DataFrame,
                         gpudb_table: GPUdbTable,
                         batch_size: int = BATCH_SIZE,
                         show_progress: bool = False) -> int:
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
        converted_df = cls._table_convert_df_for_insert(df, gpudb_table=gpudb_table)

        cls._LOG.debug(f"Inserting rows into <{gpudb_table.table_name}>")
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

        gpudb_table.flush_data_to_server()
        rows_inserted = gpudb_table.size() - rows_before
        cls._LOG.debug(f"Rows inserted: {rows_inserted}")
        return rows_inserted


    @classmethod
    @typechecked
    def _table_convert_df_for_insert(cls, df: pd.DataFrame, gpudb_table: GPUdbTable) -> pd.DataFrame:
        """ Convert dataframe for insert into Kinetica table. """
        data_list = []

        col_properties = gpudb_table.get_table_type().column_properties
        cls._LOG.debug(f"col properties: {col_properties}")

        for col_name, col_data in df.items():
            ref_val = col_data.loc[col_data.first_valid_index()]

            if isinstance(ref_val, pd.Timestamp):
                if (GPUdbColumnProperty.TIMESTAMP in col_properties[col_name]):
                    col_data = col_data.astype(np.int64) // int(1e6)
                elif (GPUdbColumnProperty.DATETIME in col_properties[col_name]):
                    col_data = col_data.astype(np.str)
                else:
                    raise GPUdbException(f"Can't convert {col_name} timestamp field to the target column type")
            elif isinstance(ref_val, list) or isinstance(ref_val, np.ndarray):
                col_data = col_data.map(cls.vec_to_bytes)
            data_list.append(col_data)

        return pd.concat(data_list, axis=1)

    TYPE_NUMPY_TO_GPUDB = {
        'int64':           [_COL_TYPE.LONG],
        'int32':           [_COL_TYPE.INT],
        'int16':           [_COL_TYPE.INT, GPUdbColumnProperty.INT16],
        'int8':            [_COL_TYPE.INT, GPUdbColumnProperty.INT8],
        'float64':         [_COL_TYPE.DOUBLE],
        'float32':         [_COL_TYPE.FLOAT],
        'datetime64[ns]':  [_COL_TYPE.LONG, GPUdbColumnProperty.TIMESTAMP],
        'uint64':          [_COL_TYPE.STRING, GPUdbColumnProperty.ULONG],
        'bool':            [_COL_TYPE.INT, GPUdbColumnProperty.BOOLEAN],
        'Int64':           [_COL_TYPE.LONG],
        'Float32':         [_COL_TYPE.FLOAT]
    }


    @classmethod
    @typechecked
    def _table_types_from_df(cls, df: pd.DataFrame,
                             col_type_override: dict) -> List:
        """ Create GPUdb column types from a DataFrame. """
        type_list = []

        # create a copy because we will be modifying this.
        col_type_override = col_type_override.copy()

        for col_name, col_data in df.items():
            np_type = col_data.dtype.name
            col_type = cls.TYPE_NUMPY_TO_GPUDB.get(np_type)
            col_type_attr = None

            if col_type is not None:
                col_type_base = col_type[0]
                col_type_attr = col_type[1:]
            else:
                # need to inspect the type directly
                ref_val = col_data.loc[col_data.first_valid_index()]

                if isinstance(ref_val, str):
                    col_type_base = cls._COL_TYPE.STRING
                    max_len = col_data.map(lambda x: len(x) if x is not None else 0).max()
                    max_len = max(max_len,2)
                    spow = 2 ** ceil(log2(max_len))
                    if(spow <= 256):
                        col_type_attr = [f'char{spow}']
                elif isinstance(ref_val, list) or isinstance(ref_val, np.ndarray):
                    vec_dim = len(ref_val)
                    col_type_base = cls._COL_TYPE.BYTES
                    col_type_attr = [f'vector({vec_dim})']
                else:
                    raise GPUdbException(f"{col_name}: Type not supported: {type(ref_val)}")
            
                # only add nullable if the type is string or vector
                has_null = col_data.isnull().any()
                if has_null:
                    col_type_attr.append(GPUdbColumnProperty.NULLABLE)
                
            # replace the column attributes, if provided
            col_attr_override = col_type_override.pop(col_name, None)
            if (col_attr_override is not None):
                if isinstance(col_attr_override, str):
                    col_type_attr = [prop.strip() for prop in col_attr_override.split(',')]
                elif isinstance(col_attr_override, list):
                    col_type_attr =  col_attr_override
                else:
                    raise GPUdbException(f"{col_attr_override}: Type properties not supported: {type(col_attr_override)}")

            type_def = [col_name, col_type_base]
            
            if col_type_attr:
                type_def += col_type_attr

            type_list.append(type_def)

        if(len(col_type_override) > 0):
            raise GPUdbException(f"Column type map has unknown columns: {list(col_type_override.keys())}")

        return type_list
    
# end class DataFrameUtils
