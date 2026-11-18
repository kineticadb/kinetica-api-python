#!/usr/bin/env python3
"""
Example Python program that maps a dataclass to a table's type with a
TypeObjectMap: the type is made from the class, objects of the class are
inserted, and records come back as objects of the class.

Another client registers a class of its own for the table's type, found out
from the table.

Run with KINETICA_URL, USERNAME and PASSWORD set for your database.
"""
import dataclasses
import datetime
import os
from decimal import Decimal
from typing import Optional

from gpudb import GPUdb, GPUdbTable, TypeObjectMap

KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")
TABLE_NAME = "example_type_object_map"


@dataclasses.dataclass
class Order:
    # A column's properties, and if need be its type, go with the field
    order_id: int = TypeObjectMap.column("primary_key", column_type="int")
    customer: str = TypeObjectMap.column("char32")
    placed: Optional[datetime.datetime] = None
    amount: Optional[Decimal] = TypeObjectMap.column("decimal(10,2)", default=None)
    paid: bool = False


@dataclasses.dataclass
class Summary:
    """Another view of the same records: fields matched to columns by name."""
    customer: str
    amount: Optional[Decimal]
    order_id: int = 0
    paid: bool = False
    placed: Optional[datetime.datetime] = None


def main():
    options = GPUdb.Options()
    options.username = USERNAME
    options.password = PASSWORD
    kdb = GPUdb(host=KINETICA_URL, options=options)
    print(f"Connected to Kinetica at {KINETICA_URL}")

    kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})
    orders = TypeObjectMap.from_class(Order)
    print("Type:", ", ".join(f"{c.name} {c.column_type} {c.column_properties}" for c in orders.type.columns))

    # Creating the type registers the map with the client
    kdb.create_table(TABLE_NAME, orders.create_type(kdb))

    try:
        table = GPUdbTable(None, name=TABLE_NAME, db=kdb)
        table.insert_records([Order(1, "acme", datetime.datetime(2026, 10, 1, 9, 30), Decimal("19.99"), True),
                              Order(2, "bolt", amount=Decimal("5.25")),
                              Order(3, "coil")])

        for order in kdb.get_records_and_decode(TABLE_NAME, options={"sort_by": "order_id"})["records"]:
            print(" ", order)

        # A table's records become objects on request
        unpaid = orders.to_objects(table.get_records(options={"expression": "paid = 0", "sort_by": "order_id"}))
        print("Unpaid:", [order.order_id for order in unpaid])

        # Another client, mapping the table's type to a class of its own
        other = GPUdb(host=KINETICA_URL, options=options)
        other.add_known_type_from_table(TABLE_NAME, Summary)
        for summary in other.get_records_and_decode(TABLE_NAME, options={"sort_by": "order_id"})["records"]:
            print(f"  {summary.customer}: {summary.amount}")
    finally:
        kdb.clear_table(TABLE_NAME, options={"no_error_if_not_exists": "true"})


if __name__ == "__main__":
    print("Kinetica Python API Type Object Map Example")
    print("=" * 50)
    main()
