import json
import os
from typing import Optional
import gpudb
from gpudb import GPUdb, GPUdbTable, AttrDict
import pandas as pd
from datetime import datetime

# Configuration
KINETICA_URL = os.getenv("KINETICA_URL", "http://192.168.16.2:9191")  # Replace with your Kinetica instance URL
USERNAME = os.getenv("USERNAME", "admin")  # Replace with your username
PASSWORD = os.getenv("PASSWORD", "Kinetica1.")  # Replace with your password
TABLE_NAME = "employees"

def connect_to_kinetica() -> Optional[GPUdb]:
    """Establish connection to Kinetica database"""
    try:
        # Create connection options
        options = gpudb.GPUdb.Options()
        options.username = USERNAME
        options.password = PASSWORD

        # Connect to Kinetica
        db = GPUdb(host=KINETICA_URL, options=options)

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
        ["salary", "double", "nullable"],
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
        table = GPUdbTable(db=db, name=table_name, use_multihead_io=True, multihead_ingest_batch_size=50)

        def generate_data():
            # Generate sample data
            large_dataset = []
            for i in range(0, 5000):  # Generate 5000 records
                record = {
                    "id": i,
                    "name": f"Employee_{i}",
                    "department": ["Engineering", "Sales", "Marketing", "HR", "Finance"][i % 5],
                    "salary": 50000.0 + (i * 100),
                    "hire_date": "2023-01-01",
                    "is_active": i % 2 == 0
                }
                large_dataset.append(record)
                if len(large_dataset) == batch_size:
                    yield large_dataset
                    large_dataset = []

        # Insert in batches using GPUdbTable.insert_records
        for i, batch in enumerate(generate_data()):
            response: GPUdbTable = table.insert_records(batch)

            latest_inserted = response.latest_insert_records_count
            latest_updated = response.latest_update_records_count
            latest_duration = response.latest_duration
            print(f"Inserted batch {i} - inserted {latest_inserted} records")
            print(f"Inserted batch {i} - updated {latest_updated} records")
            print(f"Inserted batch {i} - duration {latest_duration} seconds")

        print(f"Total records inserted: {table.total_insert_records_count}")
        print(f"Total records updated: {table.total_update_records_count}")
        print(f"Total duration: {table.total_duration}")

    except Exception as e:
        print(f"Batch insertion failed: {e}")


def main():
    """Main function to demonstrate GPUdb.insert_records usage"""
    print("Kinetica multi-head Data Insertion Example")
    print("=" * 42)

    # Connect to Kinetica
    db = connect_to_kinetica()
    if not db:
        return

    # Create sample table
    table_name = create_sample_table(db)
    if not table_name:
        return

    print("\n1. Inserting large dataset in batches using GPUdbTable.insert_records with multi-head ingest ON ...")
    multi_head_insert(db, table_name, batch_size=50)


if __name__ == "__main__":
    main()