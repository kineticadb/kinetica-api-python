import argparse
import decimal
import json
import os
from typing import Optional, Union, List
from typing_extensions import LiteralString
import gpudb
from gpudb import GPUdb, GPUdbTable, AttrDict
import pandas as pd
from datetime import datetime

# Configuration
KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")  # Replace with your Kinetica instance URL
USERNAME = os.getenv("USERNAME", "")  # Replace with your username
PASSWORD = os.getenv("PASSWORD", "")  # Replace with your password
TABLE_NAME = "employees_mh"


def connect_to_kinetica(url, username, password) -> Optional[GPUdb]:
    """Establish connection to Kinetica database"""
    try:
        # Create connection options
        options = gpudb.GPUdb.Options()
        options.username = username
        options.password = password

        # Connect to Kinetica
        db = GPUdb(host=url, options=options)

        print("Connected to Kinetica successfully!")
        return db
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

def create_sample_table(db: GPUdb):
    """Create a sample table for demonstration"""

    # Drop table if it exists
    db.clear_table(TABLE_NAME, "", {"no_error_if_not_exists": True})

    # Define table schema
    columns = """[
        ["id", "int", "primary_key"],
        ["name", "string"],
        ["department", "string"],
        ["salary", "decimal(26,13)", "nullable"],
        ["hire_date", "string"],
        ["is_active", "boolean"]
    ]"""

    try:

        # Create table
        response = db.create_table(TABLE_NAME, columns)

        # schema = GPUdbTable(name=table_name, db=db).get_table_type().record_schema
        if response['status_info']['status'] == "OK":
            print(f"Table '{TABLE_NAME}' created successfully!")
            return TABLE_NAME
    except Exception as e:
        print(f"Table creation failed: {e}")
        return None

def multi_head_insert(db: GPUdb, table_name, batch_size=1000):
    """Insert large datasets in batches using GPUdbTable.insert_records"""
    try:
        table = GPUdbTable(db=db, name=table_name, use_multihead_io=True, multihead_ingest_batch_size=batch_size, flush_multi_head_ingest_per_insertion=True)

        # Generate sample data
        large_dataset = []
        for i in range(8, 5008):  # Generate 5000 records
            record = {
                "id": i,
                "name": f"Employee_{i}",
                "department": ["Engineering", "Sales", "Marketing", "HR", "Finance"][i % 5],
                "salary": 1800345.0309635 + (i * 100),  # float_to_string(1800345.0309635 + (i * 100), DEFAULT_SCALE),
                "hire_date": "2023-01-01",
                "is_active": i % 2 == 0
            }
            print(record)
            large_dataset.append(record)

        # Insert in batches using GPUdb.insert_records
        total_inserted = 0

        # Insert in batches using GPUdbTable.insert_records
        j = 0
        for i in range(0, len(large_dataset), 500):
            j += 1
            batch = large_dataset[i:i + 500]
            # print(batch)
            response: GPUdbTable = table.insert_records(batch)
            table.flush_data_to_server()

            latest_inserted = response.latest_insert_records_count
            latest_updated = response.latest_update_records_count
            latest_duration = response.latest_duration
            print(f"Inserted batch {j} - inserted {latest_inserted} records")
            print(f"Inserted batch {j} - updated {latest_updated} records")
            print(f"Inserted batch {j} - duration {latest_duration} seconds")

        print(f"Total records inserted: {table.total_insert_records_count}")
        print(f"Total records updated: {table.total_update_records_count}")
        print(f"Total duration: {table.total_duration}")

    except Exception as e:
        print(f"Batch insertion failed: {e}")


def main(url, username, password):
    """Main function to demonstrate GPUdb.insert_records usage"""
    print("Kinetica multi-head Data Insertion Example")
    print("=" * 42)

    # Connect to Kinetica
    db = connect_to_kinetica(url, username, password)
    if not db:
        return

    # Create sample table
    table_name = create_sample_table(db)
    if not table_name:
        return

    print("\n1. Inserting large dataset in batches using GPUdbTable.insert_records with multi-head ingest ON ...")
    multi_head_insert(db, table_name )


if __name__ == "__main__":

    # Set up args
    parser = argparse.ArgumentParser(description='Run multi-head insert example.')
    parser.add_argument('--url', default=KINETICA_URL, help='Kinetica URL to run example against')
    parser.add_argument('--username', default=USERNAME, help='Username of user to run example with')
    parser.add_argument('--password', default=PASSWORD, help='Password of user')

    args = parser.parse_args()

    main(args.url, args.username, args.password)
