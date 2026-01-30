import os
import argparse
from typing import Optional

import json
import pandas as pd

from gpudb import GPUdb, GPUdbTable, AttrDict

# Configuration
KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")  # Replace with your Kinetica instance URL
USERNAME = os.getenv("USERNAME", "")  # Replace with your username
PASSWORD = os.getenv("PASSWORD", "")  # Replace with your password
TABLE_NAME = "employee"



def connect_to_kinetica(url, username, password) -> Optional[GPUdb]:
    """Establish connection to Kinetica database"""
    try:
        # Create connection options
        options = GPUdb.Options()
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
        ["salary", "decimal(10,2)", "nullable"],
        ["hire_date", "string"],
        ["is_active", "boolean"]
    ]"""

    try:
        # Create table
        response = db.create_table(TABLE_NAME, columns)
        if response['status_info']['status'] == "OK":
            print(f"Table '{TABLE_NAME}' created successfully!")
            return TABLE_NAME
    except Exception as e:
        print(f"Table creation failed: {e}")
        return None

def insert_single_record(db: GPUdb, table_name):
    """Insert a single record using GPUdb.insert_records"""
    try:
        # Single record data
        record = {
            "id": 1,
            "name": "John Doe",
            "department": "Engineering",
            "salary": 50000.98,
            "hire_date": "2023-01-15",
            "is_active": True
        }

        # Insert record using GPUdb.insert_records
        response = db.insert(table_name=table_name, records=[record])

        print(f"Inserted single record. Count inserted: {response['count_inserted']}")

    except Exception as e:
        print(f"Single record insertion failed: {e}")

def insert_multiple_records(db: GPUdb, table_name):
    """Insert multiple records using GPUdb.insert_records"""
    try:
        # Multiple records data
        records = [
            {
                "id": 2,
                "name": "Jane Smith",
                "department": "Marketing",
                "salary": 65000.00,
                "hire_date": "2023-02-20",
                "is_active": True
            },
            {
                "id": 3,
                "name": "Bob Johnson",
                "department": "Sales",
                "salary": None,
                "hire_date": "2023-03-10",
                "is_active": False
            },
            {
                "id": 4,
                "name": "Alice Brown",
                "department": "Engineering",
                "salary": 80000.01,
                "hire_date": "2023-01-05",
                "is_active": True
            }
        ]

        # Insert multiple records using GPUdb.insert_records
        response = db.insert(
            table_name=table_name,
            records=records
        )
        if response['status_info']['status'] == "OK":
            print(f"Inserted {response['count_inserted']} records")

    except Exception as e:
        print(f"Multiple records insertion failed: {e}")

def insert_from_pandas(db: GPUdb, table_name):
    """Insert data from a pandas DataFrame using GPUdb.insert_records"""
    try:
        # Create sample DataFrame
        df = pd.DataFrame({
            'id': [5, 6, 7],
            'name': ['Charlie Wilson', 'Diana Prince', 'Eddie Murphy'],
            'department': ['HR', 'Finance', 'IT'],
            'salary': [60000.23, 70000.04, 850000.03],
            'hire_date': ['2023-04-01', '2023-05-15', '2023-06-20'],
            'is_active': [True, True, False]
        })

        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')

        # Insert records using GPUdb.insert_records
        response = db.insert(
            table_name=table_name,
            records=records
        )

        if response['status_info']['status'] == "OK":
            print(f"Inserted {response['count_inserted']} records")

    except Exception as e:
        print(f"DataFrame insertion failed: {e}")

def insert_with_batch_size(db: GPUdb, table_name, batch_size=1000):
    """Insert large datasets in batches using GPUdb.insert_records"""
    try:
        # Generate sample data
        large_dataset = []
        for i in range(8, 58):  # Generate 5000 records
            record = {
                "id": i,
                "name": f"Employee_{i}",
                "department": ["Engineering", "Sales", "Marketing", "HR", "Finance"][i % 5],
                "salary": 1500001.03 + (i * 100),
                "hire_date": "2023-01-01",
                "is_active": i % 2 == 0
            }
            large_dataset.append(record)

        # Insert in batches using GPUdb.insert_records
        total_inserted = 0
        for i in range(0, len(large_dataset), batch_size):
            batch = large_dataset[i:i + batch_size]
            response = db.insert(
                table_name=table_name,
                records=batch
            )
            if response['status_info']['status'] == "OK":
                total_inserted += response['count_inserted']
                print(f"Inserted batch {i//batch_size + 1}: {response['count_inserted']} records")

        print(f"Total records inserted: {total_inserted}")

    except Exception as e:
        print(f"Batch insertion failed: {e}")

def insert_with_options(db: GPUdb, table_name):
    """Insert records with additional options using GPUdb.insert_records"""
    try:
        # Sample records
        records = [
            {
                "id": 10001,
                "name": "Test User 1",
                "department": "QA",
                "salary": f"{72000.0}",
                "hire_date": "2023-07-01",
                "is_active": True
            },
            {
                "id": 10002,
                "name": "Test User 2",
                "department": "QA",
                "salary": f"{74000.0}",
                "hire_date": "2023-07-15",
                "is_active": True
            }
        ]

        # Insert with options
        options = {
            "return_record_ids": "true",
            "update_on_existing_pk": "true"
        }

        response = db.insert(
            table_name=table_name,
            records=records,
            options=options
        )
        if response['status_info']['status'] == "OK":
            print(f"Inserted {response['count_inserted']} records")

            if 'record_ids' in response:
                print(f"Record IDs: {response['record_ids']}")

    except Exception as e:
        print(f"Insertion with options failed: {e}")


def update_records(db: GPUdb, table_name):
    response = db.update(table_name=table_name, expression="id = 10001", new_values_map={"name": "'Test User 10001'"})
    print(f"Update response = {response}")

def delete_records(db: GPUdb, table_name):
    response: AttrDict = db.delete(table_name=table_name, expression="id = 10002")
    print(f"Delete response = {response}")

def agg_group_by(db: GPUdb):
    table_employees = GPUdbTable(_type=None, name=TABLE_NAME, db=db)
    resp = table_employees.aggregate_group_by(
            column_names = ["department", "name", "SUM(salary)"],
            offset = 0,
            limit = 10,
            options = {"sort_by": "key"}
        )  # ["data"]

    print(json.dumps(resp, indent = 4))

def main(url, username, password):
    """Main function to demonstrate GPUdb.insert_records usage"""
    print("Kinetica GPUdb.insert_records Data Insert Example")
    print("=" * 55)

    # Connect to Kinetica
    db = connect_to_kinetica(url, username, password)
    if not db:
        return

    schema = None

    # Create sample table
    table_name = create_sample_table(db)
    if not table_name:
        return

    # Demonstrate different insertion methods using GPUdb.insert_records
    print("\n1. Inserting single record using GPUdb.insert_records...")
    insert_single_record(db, table_name)

    print("\n2. Inserting multiple records using GPUdb.insert_records...")
    insert_multiple_records(db, table_name)

    print("\n3. Inserting from pandas DataFrame using GPUdb.insert_records...")
    insert_from_pandas(db, table_name )

    print("\n4. Inserting large dataset in batches using GPUdb.insert_records...")
    insert_with_batch_size(db, table_name, batch_size=1000)

    print("\n5. Inserting with options using GPUdb.insert_records...")
    insert_with_options(db, table_name )

    print("\nGPUdb.insert_records examples completed!")

    update_records(db, table_name)

    delete_records(db, table_name)

    print("\n6. Aggregating data with options using GPUdbTable.aggregate_group_by...")
    agg_group_by(db)

    print("\n7. Getting data with options using GPUdb.get_records...")
    resp = db.get_records(table_name=table_name, encoding = 'json', options = {"sort_by": "id"})
    print(json.dumps(resp["records_json"], indent = 4))

if __name__ == "__main__":

    # Set up args
    parser = argparse.ArgumentParser(description='Run insert/update/delete example.')
    parser.add_argument('--url', default=KINETICA_URL, help='Kinetica URL to run example against')
    parser.add_argument('--username', default=USERNAME, help='Username of user to run example with')
    parser.add_argument('--password', default=PASSWORD, help='Password of user')

    args = parser.parse_args()

    main(args.url, args.username, args.password)
