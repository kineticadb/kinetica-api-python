#!/usr/bin/env python3
"""
Example Python program that ingests records with a GPUdbIngestor in a
``with`` block, which flushes the records still queued, and closes the
ingestor, on leaving it.

A closed ingestor takes no more records: none would ever be flushed.

Run with KINETICA_URL, USERNAME and PASSWORD set for your database.
"""
import os

from gpudb import GPUdb, GPUdbException, GPUdbRecordType
from gpudb.gpudb_multihead_io import GPUdbIngestor

KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")
TABLE_NAME = "example_ingestor_close"


def count(kdb):
    return kdb.get_records(TABLE_NAME, 0, 0)["total_number_of_records"]


def main():
    options = GPUdb.Options()
    options.username = USERNAME
    options.password = PASSWORD
    kdb = GPUdb(host=KINETICA_URL, options=options)
    print(f"Connected to Kinetica at {KINETICA_URL}")

    kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})
    record_type = GPUdbRecordType([["reading_id", "int", "primary_key"],
                                   ["sensor", "string", "char16"],
                                   ["value", "double"]])
    record_type.create_type(kdb)
    kdb.create_table(TABLE_NAME, record_type.type_id)

    try:
        # A batch of 1000 records a rank: 2500 records fill some queues, and
        # leave the rest of the records queued until the block ends
        with GPUdbIngestor(kdb, TABLE_NAME, record_type, batch_size=1000) as ingestor:
            for i in range(2500):
                ingestor.insert_record([i, f"sensor{i % 8}", i * 0.25])
            print(f"In the block: {count(kdb)} records inserted so far")

        print(f"After the block: {count(kdb)} records inserted; the ingestor is closed: {ingestor.closed}")

        try:
            ingestor.insert_record([9999, "late", 0.0])
        except GPUdbException as e:
            print(f"Refused: {e}")
    finally:
        kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})


if __name__ == "__main__":
    print("Kinetica Python API Ingestor Close Example")
    print("=" * 50)
    main()
