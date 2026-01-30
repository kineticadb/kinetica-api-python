#!/usr/bin/env python3
"""
Example Python program to insert data into a Kinetica table using Python datetime objects.
This demonstrates how to work with the Kinetica Python API and handle datetime data.
"""
import os

import argparse
from datetime import datetime, timezone
import random
import time
from gpudb import GPUdb, GPUdbTable

# Configuration
KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")  # Replace with your Kinetica instance URL
USERNAME = os.getenv("USERNAME", "")  # Replace with your username
PASSWORD = os.getenv("PASSWORD", "")  # Replace with your password
TABLE_NAME = "example_datetime_table"

# Define table schema with datetime columns
TABLE_SCHEMA = """[
    ["id", "int", "primary_key"],
    ["name", "string"],
    ["created_at", "datetime"],
    ["updated_at", "timestamp"],
    ["event_time", "datetime"],
    ["insertion_time", "time", "nullable"],
    ["insertion_date", "date", "nullable"],
    ["value", "double"]
]"""

def create_kinetica_connection(url, username, password):
    """Create and return a connection to Kinetica."""
    try:
        # Create connection options
        options = GPUdb.Options()
        options.username = username
        options.password = password

        # Create connection
        kdb = GPUdb(host=url, options=options)

        print(f"Connected to Kinetica at {url}")
        return kdb

    except Exception as e:
        print(f"Error connecting to Kinetica: {e}")
        return None

def create_table(kdb):
    """Create a table with datetime columns."""
    try:

        # type_id: object = kdb.create_type(type_definition=TABLE_SCHEMA, label="test_type_1")["type_id"]
        kdb.clear_table(TABLE_NAME, "", {"no_error_if_not_exists": True})

        # Create table if it doesn't exist
        response = kdb.create_table(TABLE_NAME, TABLE_SCHEMA)['status_info']['status'] == "OK"
        if response:
            print(f"Table '{TABLE_NAME}' created successfully")
        return response

    except Exception as e:
        print(f"Error creating table: {e}")
        return False

def insert_datetime_data(kdb):
    """Insert sample data with Python datetime objects."""
    try:
        # Sample data with Python datetime objects
        sample_data = [
            {
                "id": 1,
                "name": "Event A",
                "created_at": datetime.now(),
                "updated_at": datetime.now(timezone.utc).timestamp(),
                "event_time": datetime(2024, 1, 15, 10, 30, 0),
                "value": 123.45
            },
            {
                "id": 2,
                "name": "Event B",
                "created_at": datetime(2024, 2, 20, 14, 15, 30),
                "updated_at": datetime(2024, 2, 20, 14, 15, 30, tzinfo=timezone.utc).timestamp(),
                "event_time": datetime.now(),
                "insertion_time": datetime.fromtimestamp(time.time()).strftime('%H:%M:%S'),
                "insertion_date": None,
                "value": 678.90
            },
            {
                "id": 3,
                "name": "Event C",
                "created_at": datetime.now(),
                "updated_at": datetime.now(timezone.utc).timestamp(),
                "event_time": datetime(2024, 3, 10, 9, 0, 0),
                "insertion_date": datetime.now().date(),
                "value": 456.78
            }
        ]

        # Insert data into the table
        response = kdb.insert(
            table_name=TABLE_NAME,
            records=sample_data,
            options={"update_on_existing_pk": "true"}
        )
        if response['status_info']['status'] == "OK":
            print(f"Count inserted: {response['count_inserted']}")
            return True
        else:
            return False

    except Exception as e:
        print(f"Error inserting data: {e}")
        return False

def insert_datetime_data_lists(kdb):
    try:
        sample_data = [
            [
                4,
                "Event D",
                datetime.now(),
                datetime.now(timezone.utc).timestamp(),
                datetime(2024, 1, 15, 10, 30, 0),
                time.time(),
                None,
                123.45
            ],
            [
                5,
                "Event E",
                datetime(2024, 2, 20, 14, 15, 30),
                datetime(2024, 2, 20, 14, 15, 30, tzinfo=timezone.utc).timestamp(),
                datetime.now(),
                None,
                datetime.now().date(),
                678.90
            ],
            [
                6,
                "Event F",
                datetime.now(),
                datetime.now(timezone.utc).timestamp(),
                datetime(2024, 3, 10, 9, 0, 0),
                datetime.fromtimestamp(time.time()).strftime('%H:%M:%S'),
                None,
                456.78
            ]
        ]
        # Insert data into the table
        response = kdb.insert(
            table_name=TABLE_NAME,
            records=sample_data,
            options={"update_on_existing_pk": "true"}
        )
        if response['status_info']['status'] == "OK":
            print(f"Count inserted: {response['count_inserted']}")
            return True
        else:
            return False

    except Exception as e:
        print(f"Error inserting data: {e}")
        return False


def query_datetime_data(kdb):
    """Query and display the inserted datetime data."""
    try:
        # Query the table
        table = GPUdbTable(name=TABLE_NAME, db=kdb)
        response = table.get_records(
            offset=0,
            limit=100,
            options={}
        )

        if len(response) > 0:
            print(f"\nQueried {len(response)} records:")
            print("-" * 80)

            for record in response:
                # Convert milliseconds back to datetime for display
                created_at = record['created_at']
                updated_at = datetime.fromtimestamp(record['updated_at']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                event_time = record['event_time']
                insertion_time = record['insertion_time']
                insertion_date = record['insertion_date']

                print(f"ID: {record['id']}")
                print(f"Name: {record['name']}")
                print(f"Created At: {created_at}")
                print(f"Updated At: {updated_at}")
                print(f"Event Time: {event_time}")
                print(f"Insertion Time: {insertion_time}")
                print(f"Insertion Date: {insertion_date}")
                print(f"Value: {record['value']}")
                print("-" * 40)

            return True

    except Exception as e:
        print(f"Error querying data: {e}")
        return False

def insert_bulk_datetime_data(kdb, num_records=100):
    """Insert bulk data with random datetime values."""
    try:
        bulk_data = []
        base_time = datetime.now()

        for i in range(num_records):
            # Generate random datetime within the last 30 days
            random_seconds = random.randint(0, 30 * 24 * 60 * 60)
            random_datetime = datetime.fromtimestamp(base_time.timestamp() - random_seconds)

            record = {
                "id": i + 100,  # Start from 100 to avoid conflicts
                "name": f"Bulk Event {i}",
                "created_at": random_datetime,
                "updated_at": datetime.now().timestamp(),
                "event_time": random_datetime,
                "insertion_time": time.time(),
                "insertion_date": datetime.now().date(),
                "value": round(random.uniform(10.0, 1000.0), 2)
            }
            bulk_data.append(record)

        # Insert bulk data
        response = kdb.insert(
            table_name=TABLE_NAME,
            records=bulk_data,
            options={"update_on_existing_pk": "true"}
        )
        if response['status_info']['status'] == "OK":
            print(f"Count inserted: {response['count_inserted']}")
            return True
        else:
            return False

    except Exception as e:
        print(f"Error inserting bulk data: {e}")
        return False

def main(url, username, password):
    """Main function to demonstrate datetime insertion."""
    print("Kinetica Python API DateTime Insert Example")
    print("=" * 50)

    # Create connection
    kdb = create_kinetica_connection(url, username, password)
    if not kdb:
        return

    # Create table
    if not create_table(kdb):
        return

    # Insert sample data with datetime objects
    print("Inserting data as list of dicts")
    if insert_datetime_data(kdb):
        print("Sample data inserted successfully!")

    print("Inserting data as list of lists")
    if insert_datetime_data_lists(kdb):
        print("Sample data as lists inserted successfully")

    # Insert bulk data (optional)
    print("\nInserting bulk data...")
    if insert_bulk_datetime_data(kdb, 50):
        print("Bulk data inserted successfully!")

    # Query and display data
    query_datetime_data(kdb)

    print("\nExample completed successfully!")

if __name__ == "__main__":

    # Set up args
    parser = argparse.ArgumentParser(description='Run insert date/time example.')
    parser.add_argument('--url', default=KINETICA_URL, help='Kinetica URL to run example against')
    parser.add_argument('--username', default=USERNAME, help='Username of user to run example with')
    parser.add_argument('--password', default=PASSWORD, help='Password of user')

    args = parser.parse_args()

    main(args.url, args.username, args.password)
