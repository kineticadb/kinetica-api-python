#!/usr/bin/env python3
"""
Example Python program that looks records up with a RecordRetriever: by key,
by key and expression, by expression alone, page by page, and by column.

Lookups by key go straight to the worker rank holding the key; on a replicated
table, every lookup with an expression goes to a worker rank.

Run with KINETICA_URL, USERNAME and PASSWORD set for your database.
"""
import os

from gpudb import GPUdb, GPUdbRecordType, GPUdbTable
from gpudb.gpudb_multihead_io import RecordRetriever

KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")
TABLE_NAME = "example_retriever_lookups"


def main():
    options = GPUdb.Options()
    options.username = USERNAME
    options.password = PASSWORD
    kdb = GPUdb(host=KINETICA_URL, options=options)
    print(f"Connected to Kinetica at {KINETICA_URL}")

    kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})
    record_type = GPUdbRecordType([["order_id", "int", "primary_key"],
                                   ["customer", "string", "char16"],
                                   ["amount", "double"]])
    record_type.create_type(kdb)
    kdb.create_table(TABLE_NAME, record_type.type_id)
    GPUdbTable(None, name=TABLE_NAME, db=kdb).insert_records(
        [[i, "customer%d" % (i % 10), i * 1.5] for i in range(100)])

    try:
        # Lookups not given options of their own use the retriever's
        retriever = RecordRetriever(kdb, TABLE_NAME, record_type)
        print(f"Table is replicated: {retriever.is_table_replicated}")

        print("By key:", [dict(r) for r in retriever.get_records_by_key([42])["data"]])
        print("By key and expression:",
              len(retriever.get_records_by_key([42], "amount > 50")["data"]), "record(s)")

        matching = retriever.get_records_by_key(None, "customer = 'customer3'", options={"sort_by": "order_id"})
        print("By expression alone:", [r["order_id"] for r in matching["data"]])

        # A large match comes in pages (as many records a page as the server
        # returns at once); the next page starts at the offset after the last
        retriever.options = {"sort_by": "order_id"}
        page = retriever.get_records_by_key(None, "amount >= 0", offset=90)
        print(f"From offset 90 of {page['total_number_of_records']}:", [r["order_id"] for r in page["data"]])
        retriever.options = None

        columns = retriever.get_columns_by_key(["customer", "amount * 2 as doubled"], [42])
        print("Columns by key:", [dict(r) for r in columns["data"]])
    finally:
        kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})


if __name__ == "__main__":
    print("Kinetica Python API Record Retriever Lookups Example")
    print("=" * 50)
    main()
