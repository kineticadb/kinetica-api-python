# ---------------------------------------------------------------------------
# gpudb_sql_context.py - Kinetica API class for creating and formatting 
# LLM SQL contexts.
#
# Copyright (c) 2024 Kinetica DB Inc.
# ---------------------------------------------------------------------------

import textwrap
from collections import OrderedDict
from typing import Optional, List, Tuple, Dict


class GPUdbBaseClause:
    """ Base class for SQL context clauses. """

    def __str__(self) -> str:
        return self.format_sql()

    def format_sql(self) -> str:
        """ Format the clause as a SQL string. """
        raise NotImplementedError("format_sql must be implemented by subclass.")


class GPUdbSamplesClause(GPUdbBaseClause):
    """ This class is used to format the samples clause of a SQL Context.

    Example
    ::
        table_ctx = GPUdbBaseClause(
            table = "sa_quickstart.nyct2020",
            comment = "This table contains spatial boundaries and attributes of the New York City.",
            col_comments = dict(
                gid="This is the unique identifer for each record in the table.",
                geom="The spatial boundary in WKT format of each NTA neighborhood.",
                BoroCode="The code of the borough to which the neighborhood belongs to."),
            rules = ["Join this table using KI_FN.STXY_WITHIN() = 1",
                    "Another rule here"])
    """

    def __init__(self, samples: Optional[List[Tuple[str, str]]] = None) -> None:
        """
        Parameters:
            samples (list[tuple[str,str]]):
                A list of tuples containing question/sql pairs.
        """
        if (samples is None):
            samples = []
        self.samples = samples

    def format_sql(self) -> str:
        ctx_dict = {
            'SAMPLES': _GPUdbSqlContextFormatter._quote_dict(OrderedDict(self.samples))
        }
        return _GPUdbSqlContextFormatter._format_clause(ctx_dict)


class GPUdbTableClause(GPUdbBaseClause):
    """ This class is used to format the table clause section of a SQL context.

    Example
    ::
        samples_ctx = GPUdbBaseClause(samples = [
            ("What are the shortest, average, and longest trip lengths for each taxi vendor?",
            ""
            SELECT th.vendor_id,
                MIN(th.trip_distance) AS shortest_trip_length,
                AVG(th.h.trip_distance) AS average_trip_length,
                MAX(th.trip_distance) AS longest_trip_length
            FROM sa_quickstart.taxi_data_historical AS th
            GROUP BY th.vendor_id;
            ""),

            ("How many trips did each taxi vendor make to JFK International Airport?",
            ""
            SELECT th.vendor_id,
                COUNT(*) AS trip_count
            FROM sa_quickstart.taxi_data_historical AS th
            JOIN sa_quickstart.nyct2020 AS n_dropoff ON KI_FN.STXY_WITHIN(th.dropoff_longitude, th.dropoff_latitude, n_dropoff.geom)
            AND n_dropoff.NTAName = 'John F. Kennedy International Airport'
            GROUP BY th.vendor_id;
            ""),
            ])
    """

    def __init__(self,
                 table: str,
                 comment: Optional[str] = None,
                 rules: Optional[List[str]] = None,
                 col_comments: Optional[Dict[str, str]] = None) -> None:
        """
        Parameters:
            table (str)
                Fully qualified table name (e.g. "schema.table")

            comment (str)
                Comment for the table.

            rules (list[str])
                A list of rules that apply to the table.

            col_comments (dict[str,str])
                A dictionary with mapping of column names to colums comments.
        """
        if (comment is None):
            comment = ""
        if (rules is None):
            rules = []
        if (col_comments is None):
            col_comments = {}

        self.table = table
        self.comment = comment
        self.col_comments = col_comments
        self.rules = rules

    def format_sql(self) -> str:
        ctx_dict = {
            'TABLE': _GPUdbSqlContextFormatter._quote_sql_obj(self.table),
            'COMMENT': _GPUdbSqlContextFormatter._quote_text(self.comment),
            'RULES': _GPUdbSqlContextFormatter._quote_list(self.rules),
            'COMMENTS': _GPUdbSqlContextFormatter._quote_dict(self.col_comments)
        }
        return _GPUdbSqlContextFormatter._format_clause(ctx_dict)


class _GPUdbSqlContextFormatter:
    """ Formatter to generate a SQL context from clauses.  """

    @classmethod
    def format_sql(cls, name: str, ctx_list: List[GPUdbBaseClause]) -> str:
        """ Format a list of SQL clauses as a CREATE CONTEXT statement. 
        The result can be passed to GPUDB.execute() to create the context.

        Args:
            name (str):
                Fully qualified name of the context. (e.g. "schema.context")

            ctx_list (list[BaseClause]):
                List of SQL clauses to include in the context.

        Returns (str):
            The formatted CREATE CONTEXT statement.
        """

        str_list = [ctx.format_sql() for ctx in ctx_list]
        sql_context = ",\n".join(str_list)
        name = cls._quote_sql_obj(name)
        return f"CREATE OR REPLACE CONTEXT {name} {sql_context}"

    @classmethod
    def _quote_sql_obj(cls, obj: str) -> str:
        parts = obj.split(".")
        parts = [f'"{p}"' for p in parts]
        return ".".join(parts)

    @classmethod
    def _quote_text(cls, text: str) -> str:
        if len(text) == 0:
            return ""
        text = text.replace("'", "''").strip()
        return f"'{text}'"

    @classmethod
    def _parens(cls, lines: List[str]) -> str:
        if len(lines) == 0:
            return ""
        params_str = ','.join(lines)
        return f"( {params_str} )"

    @classmethod
    def _quote_list(cls, items: List[str]) -> str:
        lines = []
        for item in items:
            item_text = cls._quote_text(item)
            lines.append(f"\n        {item_text}")
        return cls._parens(lines)

    @classmethod
    def _quote_dict(cls, params: Dict[str, str]) -> str:
        lines = []
        for question, sql in params.items():
            question = cls._quote_text(question)
            sql = textwrap.dedent(sql)
            sql = cls._quote_text(sql)
            lines.append(f"\n        {question} = {sql}")
        return cls._parens(lines)

    @classmethod
    def _format_clause(cls, params: Dict[str, str]) -> str:
        lines = []
        for param, val in params.items():
            if len(val) == 0:
                continue
            lines.append(f"    {param} = {val}")
        context = ',\n'.join(lines)
        return f"(\n{context}\n)"


class GPUdbSqlContext:
    """ Represents a collection of clauses that make a SQL Context. 
    
    Example:
    ::
        context_sql = GPUdbSqlContext(
            name="sa_quickstart.nyc_ctx", 
            tables=[table_ctx],
            samples=samples_ctx).format_sql()
    """

    def __init__(self,
                 name: str,
                 tables: List[GPUdbTableClause],
                 samples: GPUdbSamplesClause) -> None:
        """
         Args:
            name (str):
                Fully qualified name of the context. (e.g. "schema.context")

            tables (list[TableClause]):
                A list of table clauses.

            samples (SamplesClause):
                The samples clause that provides question/answer pairs.
        """
        self.name = name
        self.samples = samples
        self.tables = tables

    def __repr__(self) -> str:
        return f"SqlContext(name={self.name})"

    def build_sql(self) -> str:
        """ Format a list of SQL clauses as a CREATE CONTEXT statement. 
        The result can be passed to GPUDB.execute() to create the context.

        Returns (str):
            The formatted CREATE CONTEXT statement.
        """
        ctx_list: List[GPUdbBaseClause] = []

        if self.tables is not None:
            ctx_list.extend(self.tables)

        if self.samples is not None:
            ctx_list.append(self.samples)

        return _GPUdbSqlContextFormatter.format_sql(name=self.name, ctx_list=ctx_list)
