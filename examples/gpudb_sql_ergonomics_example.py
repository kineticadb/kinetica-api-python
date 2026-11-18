#!/usr/bin/env python3
"""
Example Python program that runs SQL with rows read by column name, and with
query parameters given as a tuple, or as their JSON--several rows of them for
one INSERT.

Run with KINETICA_URL, USERNAME and PASSWORD set for your database.
"""
import os

from gpudb import GPUdb

KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")
TABLE_NAME = "example_sql_ergonomics"


def main():
    options = GPUdb.Options()
    options.username = USERNAME
    options.password = PASSWORD
    kdb = GPUdb(host=KINETICA_URL, options=options)
    print(f"Connected to Kinetica at {KINETICA_URL}")

    kdb.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    kdb.execute(f"CREATE TABLE {TABLE_NAME} (id INT NOT NULL, city VARCHAR(32), temp DOUBLE, PRIMARY KEY (id))")

    try:
        # Several rows from one INSERT, their parameters given as JSON
        inserted = kdb.execute(f"INSERT INTO {TABLE_NAME} VALUES ($1, $2, $3)",
                               sql_params='[[1, "Oslo", 4.5], [2, "Cairo", 31.0], [3, "Lima", 19.5], [4, "Perth", 24.0]]')
        print(f"Inserted {inserted} rows")

        # Rows read by column name; still lists, by position too
        for row in kdb.query(f"SELECT city, temp FROM {TABLE_NAME} WHERE temp > $1 ORDER BY temp",
                             sql_params=(10,), named_rows=True):
            print(f"  {row['city']}: {row['temp']} ({row[0]})")

        warmest = kdb.query_one(f"SELECT city, temp FROM {TABLE_NAME} ORDER BY temp DESC LIMIT 1", named_rows=True)
        print("Warmest:", warmest.as_dict())

        # The rows are plain lists unless asked for
        print("Plain rows:", list(kdb.query(f"SELECT id, city FROM {TABLE_NAME} WHERE id <= $1 ORDER BY id",
                                            sql_params="[2]")))
    finally:
        kdb.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")


if __name__ == "__main__":
    print("Kinetica Python API SQL Ergonomics Example")
    print("=" * 50)
    main()
