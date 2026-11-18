#!/usr/bin/env python3
"""
Example Python program that ingests JSON records with a GPUdbIngestor, each
worker rank taking its own batch, and then upserts some of them.

When it is made, the ingestor checks that the user may insert into the table
(and update it, to upsert), and finds out whether the table is replicated; a
replicated table takes its records at the head node.  JSON records are not
checked as they are queued unless the 'validate' JSON option is set.

Run with KINETICA_URL, USERNAME and PASSWORD set for your database.
"""
import json
import os

from gpudb import GPUdb, GPUdbException, GPUdbRecordType
from gpudb.gpudb_multihead_io import GPUdbIngestor

KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")
TABLE_NAME = "example_ingestor_json"


def main():
    options = GPUdb.Options()
    options.username = USERNAME
    options.password = PASSWORD
    kdb = GPUdb(host=KINETICA_URL, options=options)
    print(f"Connected to Kinetica at {KINETICA_URL}")

    kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})
    record_type = GPUdbRecordType([["sensor_id", "int", "primary_key"],
                                   ["location", "string", "char16"],
                                   ["reading", "double"]])
    record_type.create_type(kdb)
    kdb.create_table(TABLE_NAME, record_type.type_id)

    try:
        ingestor = GPUdbIngestor(kdb, TABLE_NAME, record_type, batch_size=50, json_ingestion=True)
        print(f"Table is replicated: {ingestor.is_table_replicated}; "
              f"worker ranks: {len(ingestor.worker_list.get_worker_urls())}")

        for i in range(200):
            ingestor.insert_record(json.dumps({"sensor_id": i, "location": f"site{i % 4}", "reading": i * 0.5}))
        ingestor.flush()
        print(f"Inserted {ingestor.count_inserted} records")

        # Upserting needs the update permission too, which the ingestor checks
        # when it is made; records are checked as they are queued, on request
        upserter = GPUdbIngestor(kdb, TABLE_NAME, record_type, batch_size=50,
                                 options={"update_on_existing_pk": "true"},
                                 json_ingestion=True, json_options={"validate": True})
        try:
            upserter.insert_record('{"sensor_id": 7, "location": "site3", "reading": ')
        except GPUdbException as e:
            print(f"Refused: {e}")
        for i in range(0, 200, 20):
            upserter.insert_record(json.dumps({"sensor_id": i, "location": "moved", "reading": -1.0}))
        upserter.flush()

        moved = kdb.get_records(TABLE_NAME, 0, 100, encoding="json",
                                options={"expression": "location = 'moved'", "sort_by": "sensor_id"})
        print("Upserted:", [json.loads(r)["sensor_id"] for r in moved["records_json"]])
        print(f"Records in the table: {kdb.show_table(TABLE_NAME, options={'get_sizes': 'true'})['total_size']}")

        try:
            GPUdbIngestor(kdb, "example_no_such_table", record_type, batch_size=50, json_ingestion=True)
        except GPUdbException as e:
            print(f"Missing table: {e}")
    finally:
        kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})


if __name__ == "__main__":
    print("Kinetica Python API Ingestor JSON Example")
    print("=" * 50)
    main()
