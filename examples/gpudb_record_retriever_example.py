from __future__ import print_function

import os
import argparse
from typing import Optional
import gpudb
import datetime
import json
import random
import string
import collections

# Configuration
KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")
TABLE_NAME = "example_mh_record_retriever_with_decimals"

# Define decimal type specifications
DECIMAL_SPECS = [
    # (precision, scale, description)
    (10, 2, "Currency - fits 8 bytes"),           # decimal(10,2)
    (18, 9, "High precision - fits 8 bytes"),     # decimal(18,9)
    (27, 13, "Extended precision - needs 12 bytes"), # decimal(27,13)
]

def generate_decimal_value(precision, scale, include_negative=True):
    """Generate a random decimal value matching the specification.

    Args:
        precision: Total number of digits
        scale: Number of fractional digits
        include_negative: Whether to include negative values

    Returns:
        String representation of the decimal value
    """
    max_int_digits = precision - scale

    # Randomly decide if negative (50% chance if allowed)
    is_negative = include_negative and random.random() < 0.5

    # Generate integer part
    if max_int_digits == 0:
        int_part = "0"
    else:
        num_int_digits = random.randint(1, max_int_digits)
        int_part = str(random.randint(1, 9))
        if num_int_digits > 1:
            int_part += ''.join([str(random.randint(0, 9)) for _ in range(num_int_digits - 1)])

    # Generate fractional part
    if scale == 0:
        frac_part = ""
    else:
        num_frac_digits = random.randint(1, scale)
        frac_part = "." + ''.join([str(random.randint(0, 9)) for _ in range(num_frac_digits)])

    # Construct the value
    value = ("-" if is_negative else "") + int_part + frac_part

    return value


def run_record_retriever(db: gpudb.GPUdb):
    """Test multi-head retrieval with simplified schema including decimal types.

    This demonstrates the record retriever functionality with a simple schema
    containing one int, one char, one datetime, one time, and three decimal fields
    (all as shard keys).
    """

    # Clear table if exists
    db.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})

    # Build simple table type with ALL fields as shard keys
    _type = [
        ["id", "int"],
        ["name", "string", "shard_key", "nullable", "char16"],
        ["created_date", "string", "shard_key", "nullable", "datetime"],
        ["created_time", "string", "shard_key", "nullable", "time"],
    ]

    # Add decimal fields dynamically - ALL as shard keys
    decimal_field_names = []
    print("\n" + "="*80)
    print("ADDING DECIMAL FIELDS (ALL AS SHARD KEYS)")
    print("="*80)
    for i, (precision, scale, desc) in enumerate(DECIMAL_SPECS, 1):
        field_name = f"amount_{i}"
        decimal_field_names.append(field_name)
        _type.append([
            field_name,
            "string",
            "shard_key",  # DECIMAL FIELD IS SHARD KEY
            "nullable",
            f"decimal({precision},{scale})"
        ])
        print(f"✓ {field_name} - decimal({precision},{scale}) - {desc} [SHARD KEY]")

    # Create table with multi-head I/O enabled
    from gpudb import GPUdbTable
    table = GPUdbTable(_type, TABLE_NAME, db=db, use_multihead_io=True)

    print("\n" + "="*80)
    print("TABLE SCHEMA - SHARD KEY FIELDS")
    print("="*80)
    columns = table.gpudbrecord_type.columns

    print("\nNon-Decimal Shard Keys:")
    print(f"  name (char16) - shard_key")
    print(f"  created_date (datetime) - shard_key")
    print(f"  created_time (time) - shard_key")

    print("\nDecimal Shard Keys:")
    for col in columns:
        if col.is_decimal:
            print(f"  {col.name:<15} decimal({col.precision},{col.scale}) - shard_key")
            print(f"    Max Int Digits: {col.precision - col.scale}, Max Frac Digits: {col.scale}")

    print(f"\nTable Name: {TABLE_NAME}")

    # Create indices on the shard columns
    print("\n" + "="*80)
    print("CREATING INDICES ON SHARD KEY COLUMNS")
    print("="*80)
    for field in ["name", "created_date", "created_time"] + decimal_field_names:
        table.alter_table(action="create_index", value=field)
        print(f"  ✓ Index created on: {field}")

    # Generate and insert records
    num_batches = 5
    batch_size = 10
    generated_data = generate_and_insert_data(table, batch_size, num_batches, decimal_field_names)

    # Get some records out using regular /get/records
    print("\n" + "="*80)
    print("REGULAR /get/records (fetching first record)")
    print("="*80)
    r = db.get_records(table.name, 0, 1, 'json', {"sort_by": "id"})
    output_response(r)

    # Pick a generated record for shard key retrieval
    record = generated_data[0]

    print("\n" + "="*80)
    print("SAMPLE RECORD FOR SHARD KEY RETRIEVAL")
    print("="*80)
    print(f"Non-shard key:")
    print(f"  id: {record['id']}")
    print(f"\nShard key fields:")
    print(f"  name: {record['name']}")
    print(f"  created_date: {record['created_date']}")
    print(f"  created_time: {record['created_time']}")
    for field_name in decimal_field_names:
        print(f"  {field_name}: {record[field_name]}")

    # Build shard key from the record
    # IMPORTANT: Shard key must include ALL shard key fields in order
    shard_key = [
        record["name"],
        record["created_date"],
        record["created_time"],
    ]

    # Add decimal field values to shard key (THESE ARE PART OF SHARD KEY)
    for field_name in decimal_field_names:
        shard_key.append(record[field_name])

    # Fetch records directly from worker ranks using shard key
    print("\n" + "="*80)
    print("MULTI-HEAD RETRIEVAL BY SHARD KEY")
    print("="*80)
    print(f"Shard key composition:")
    print(f"  1. name: {shard_key[0]}")
    print(f"  2. created_date: {shard_key[1]}")
    print(f"  3. created_time: {shard_key[2]}")
    for i, field_name in enumerate(decimal_field_names, 4):
        print(f"  {i}. {field_name}: {shard_key[i-1]}")

    print(f"\nTotal shard key fields: {len(shard_key)}")
    print("\nRetrieving records directly from worker ranks...")

    # Note: This function can be provided an optional expression string
    # as the second parameter (keyword 'expression')
    output_response(table.get_records_by_key(shard_key))


def generate_and_insert_data(table, batch_size, num_batches, decimal_field_names):
    """Generate and insert test data in batches.

    Args:
        table: The table to insert data into
        batch_size: Number of records per batch
        num_batches: Number of batches to generate
        decimal_field_names: List of decimal field names

    Returns:
        List of generated records
    """
    null_percentage = 0.1
    random.seed(42)  # For reproducibility

    alphanum = string.ascii_letters + string.digits
    records = []

    print("\n" + "="*80)
    print(f"GENERATING AND INSERTING DATA")
    print(f"Batches: {num_batches}, Batch Size: {batch_size}, Total: {num_batches * batch_size}")
    print("="*80)

    # Print decimal field generation strategy
    print("\nDecimal Field Generation Strategy:")
    for i, (precision, scale, desc) in enumerate(DECIMAL_SPECS):
        field_name = decimal_field_names[i]
        print(f"\n  {field_name}:")
        print(f"    Type: decimal({precision},{scale})")
        print(f"    Description: {desc}")
        print(f"    Max integer digits: {precision - scale}")
        print(f"    Max fractional digits: {scale}")
        print(f"    Generates both positive and negative values")
        print(f"    Part of shard key: YES")

    print("\n" + "-"*80)

    # Outer loop - batches
    for i in range(num_batches):
        print(f"Processing batch {i+1}/{num_batches}...")

        # Inner loop - records per batch
        for j in range(batch_size):
            record = collections.OrderedDict()

            # ID field (NOT a shard key)
            record["id"] = (i * batch_size) + j + 1

            # Name field (char16) - SHARD KEY
            if random.random() < null_percentage:
                record["name"] = None
            else:
                name_length = random.randint(5, 16)
                record["name"] = ''.join([random.choice(alphanum) for _ in range(name_length)])

            # DateTime field - SHARD KEY
            if random.random() < null_percentage:
                record["created_date"] = None
            else:
                date_part = datetime.date(
                    random.randint(2020, 2024),
                    random.randint(1, 12),
                    random.randint(1, 28)
                ).strftime("%Y-%m-%d")

                time_part = datetime.time(
                    random.randint(0, 23),
                    random.randint(0, 59),
                    random.randint(0, 59)
                ).strftime("%H:%M:%S")

                milliseconds = f".{random.randint(0, 999):03d}"
                record["created_date"] = f"{date_part} {time_part}{milliseconds}"

            # Time field - SHARD KEY
            if random.random() < null_percentage:
                record["created_time"] = None
            else:
                time_part = datetime.time(
                    random.randint(0, 23),
                    random.randint(0, 59),
                    random.randint(0, 59)
                ).strftime("%H:%M:%S")
                milliseconds = f".{random.randint(0, 999):03d}"
                record["created_time"] = f"{time_part}{milliseconds}"

            # Generate decimal values - ALL ARE SHARD KEYS
            for idx, (precision, scale, desc) in enumerate(DECIMAL_SPECS):
                field_name = decimal_field_names[idx]

                if random.random() < null_percentage:
                    record[field_name] = None
                else:
                    record[field_name] = generate_decimal_value(
                        precision,
                        scale,
                        include_negative=True
                    )

            # Insert record
            table.insert_records(record)
            records.append(record)

    # Flush all data to server
    table.flush_data_to_server()

    print(f"\n{'='*80}")
    print(f"DATA INSERTION COMPLETE")
    print(f"{'='*80}")
    print(f"Total records inserted: {num_batches * batch_size}")
    print(f"Table name: {table.name}")

    # Print sample records with shard key annotation
    print("\n" + "="*80)
    print("SAMPLE GENERATED RECORDS (first 3)")
    print("="*80)
    for idx in range(min(3, len(records))):
        print(f"\nRecord {idx+1}:")
        rec = records[idx]
        print(f"  id: {rec['id']} [NOT shard key]")
        print(f"  name: {rec['name']} [SHARD KEY]")
        print(f"  created_date: {rec['created_date']} [SHARD KEY]")
        print(f"  created_time: {rec['created_time']} [SHARD KEY]")
        for field_name in decimal_field_names:
            print(f"  {field_name}: {rec[field_name]} [SHARD KEY - DECIMAL]")

    print(f"{'='*80}\n")

    return records


def output_response(r):
    """Pretty print the response from Kinetica API calls."""
    sr = ""
    for key in r:
        val = r[key]
        skey = '"' + key + '": '
        sval = None

        if isinstance(val, list):
            if key == 'records_json':
                sval = ''
                for rec in val:
                    sval += '\n' + json.dumps(json.loads(rec), indent=4) + ','
                sval = '[\n' + sval.replace('\n', '\n    ')[1:-1] + '\n]'
            elif key == 'records_binary':
                sval = str(val)
            else:
                sval = ''
                for rec in val:
                    sval += '\n' + json.dumps(rec.as_dict(), indent=4) + ','
                sval = '[\n' + sval.replace('\n', '\n    ')[1:-1] + '\n]'
        elif isinstance(val, dict):
            sval = json.dumps(val, indent=4)
        else:
            try:
                sval = json.dumps(json.loads(val), indent=4)
            except:
                if key == 'record_type':
                    sval = str(val)
                else:
                    sval = ' "' + str(val) + '"'

        sr += '\n' + skey + sval + ','

    print('{\n' + sr.replace('\n', '\n    ')[1:-1] + '\n}')


def connect_to_kinetica(url, username, password) -> Optional[gpudb.GPUdb]:
    """Establish connection to Kinetica database"""
    try:
        options = gpudb.GPUdb.Options()
        options.username = username
        options.password = password

        db = gpudb.GPUdb(host=url, options=options)
        print("="*80)
        print("Connected to Kinetica successfully!")
        print("="*80)
        return db
    except Exception as e:
        print(f"Connection failed: {e}")
        return None


def main(url, username, password):
    """Main function to demonstrate record retriever with decimal types as shard keys"""
    print("="*80)
    print("Kinetica Record Retriever Example")
    print("Simplified Schema with Decimal Types as Shard Keys")
    print("="*80)

    db = connect_to_kinetica(url, username, password)
    if not db:
        return

    run_record_retriever(db)

    print("\n" + "="*80)
    print("Record Retriever Test Completed Successfully!")
    print("="*80)
    print("\nKey Demonstration Points:")
    print("  ✓ Created table with 3 decimal fields as shard keys")
    print("  ✓ decimal(10,2)  - fits in 8 bytes")
    print("  ✓ decimal(18,9)  - fits in 8 bytes")
    print("  ✓ decimal(27,13) - requires 12 bytes")
    print("  ✓ Generated positive and negative decimal values")
    print("  ✓ Retrieved records using regular API")
    print("  ✓ Retrieved records using multi-head shard key (including decimals)")
    print("="*80)


if __name__ == "__main__":

    # Set up args
    parser = argparse.ArgumentParser(description='Run multi-head egress example.')
    parser.add_argument('--url', default=KINETICA_URL, help='Kinetica URL to run example against')
    parser.add_argument('--username', default=USERNAME, help='Username of user to run example with')
    parser.add_argument('--password', default=PASSWORD, help='Password of user')

    args = parser.parse_args()

    main(args.url, args.username, args.password)
