#!/usr/bin/env python3
"""
Example Python program that ingests local files into a Kinetica table through
KIFS, with GPUdbFileHandler.ingest().

ingest() uploads the files to a staging directory under the user's KIFS home
directory, loads them into the table (creating it if needed) and deletes the
staged copies, whether or not the load succeeded.

Run with KINETICA_URL, USERNAME and PASSWORD set for your database.
"""
import os
import tempfile

from gpudb import (GPUdb, GPUdbException, GPUdbFileHandler, IngestOptions, TableCreationOptions,
                   ErrorHandlingMode, FileType)

KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")
TABLE_NAME = "example_kifs_ingest"


def write_sample_files(directory):
    """Two CSV files of orders, and one with a bad record."""
    files = {
        "orders_1.csv": "order_id,customer,amount,order_date\n"
                        "1,alice,10.50,2026.10.01\n"
                        "2,bob,22.00,2026.10.02\n",
        "orders_2.csv": "order_id,customer,amount,order_date\n"
                        "3,carol,5.25,2026.10.03\n"
                        "4,dave,99.99,2026.10.04\n"
                        "5,erin,1.00,2026.10.05\n",
        "orders_bad.csv": "order_id,customer,amount,order_date\n"
                          "six,frank,7.00,2026.10.06\n"
                          "7,grace,3.50,2026.10.07\n",
    }
    for name, text in files.items():
        with open(os.path.join(directory, name), "w") as f:
            f.write(text)


def main():
    options = GPUdb.Options()
    options.username = USERNAME
    options.password = PASSWORD
    kdb = GPUdb(host=KINETICA_URL, options=options)
    print(f"Connected to Kinetica at {KINETICA_URL}")

    kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})
    handler = GPUdbFileHandler(kdb)

    with tempfile.TemporaryDirectory() as directory:
        write_sample_files(directory)

        # Every file matching a pattern, into a new table with a primary key;
        # the dates are read with their format
        result = handler.ingest(
            os.path.join(directory, "orders_[0-9].csv"),
            TABLE_NAME,
            IngestOptions(
                file_type=FileType.DELIMITED_TEXT,
                primary_keys=["order_id"],
                column_formats={"order_date": {"date": "%Y.%m.%d"}},
            ),
            TableCreationOptions(no_error_if_exists=True),
        )
        print(f"Ingested into {result.table_name}: {result.count_inserted} inserted, "
              f"{result.count_skipped} skipped")

        # By default, a bad record stops the ingest, and nothing is loaded
        try:
            handler.ingest(os.path.join(directory, "orders_bad.csv"), TABLE_NAME,
                           IngestOptions(column_formats={"order_date": {"date": "%Y.%m.%d"}}))
        except GPUdbException as ex:
            print(f"Ingest stopped at a bad record: {str(ex)[:80]}...")

        # Skipping bad records loads the rest
        result = handler.ingest(
            os.path.join(directory, "orders_bad.csv"),
            TABLE_NAME,
            IngestOptions(error_handling=ErrorHandlingMode.SKIP,
                          column_formats={"order_date": {"date": "%Y.%m.%d"}}),
        )
        print(f"With bad records skipped: {result.count_inserted} inserted, {result.count_skipped} skipped")

    records = kdb.get_records(TABLE_NAME, limit=100, encoding="json", options={"sort_by": "order_id"})
    print(f"The table now holds {records['total_number_of_records']} orders")

    kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})


if __name__ == "__main__":
    print("Kinetica Python API KIFS Ingest Example")
    print("=" * 50)
    main()
