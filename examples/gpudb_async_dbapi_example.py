import asyncio
import os
import time
from gpudb.dbapi import *
from gpudb.dbapi import aio

from gpudb.dbapi.pep249.aiopep249.connection import AsyncConnection


URL = os.getenv('PY_TEST_URL', 'http://localhost:9191')
USER = os.getenv('PY_TEST_USER', "")
PASS = os.getenv('PY_TEST_PASS', "")
SCHEMA = os.getenv('PY_TEST_SCHEMA', 'async_example')
BYPASS_SSL_CERT_CHECK = os.getenv('PY_TEST_BYPASS_CERT_CHECK', True)
if BYPASS_SSL_CERT_CHECK in ["1", 1]:
    BYPASS_SSL_CERT_CHECK = True


async def example_async():
    """async calls"""

    con1: AsyncConnection = aconnect(
        "kinetica://",
        connect_args={
            "url": URL,
            "username": USER,
            "password": PASS,
            "bypass_ssl_cert_check": BYPASS_SSL_CERT_CHECK,
        },
    )

    table_name = f'"{SCHEMA}"."async_dbapi_example"'

    create_table = f"""
    CREATE TABLE {table_name}
    (
        "i" INTEGER NOT NULL,
        "d" DOUBLE NOT NULL,
        "s" VARCHAR NOT NULL,
        "f" REAL NOT NULL,
        "l" BIGINT NOT NULL
    ) using table properties (no_error_if_exists=TRUE)"""

    await con1.execute(create_table)
    # ParamStyle - numeric_dollar
    insert_query = "insert into {} (i, d, s, f, l) values ($1, $2, $3, $4, $5)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    await con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])

    # ParamStyle - qmark
    insert_query = "insert into {} (i, d, s, f, l) values (?, ?, ?, ?, ?)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    await con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])

    # ParamStyle - numeric
    insert_query = "insert into {} (i, d, s, f, l) values (:1, :2, :3, :4, :5)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    await con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])

    # ParamStyle - format
    insert_query = "insert into {} (i, d, s, f, l) values (%i, %f, %s, %f, %l)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    await con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])

    # ParamStyle - mixed/erroneous
    insert_query = "insert into {} (i, d, s, f, l) values (%i, $2, :3, ?, %l)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    try:
        await con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])
    except ProgrammingError as e:
        print(e)

    print("QUERY ...")
    query = "select * from {}".format(table_name)
    cursor1 = await con1.execute(query)
    print(cursor1.description)

    print("WHILE loop ...")
    record = await cursor1.fetchone()
    while record:
        print(record)
        record = await cursor1.fetchone()

    print("FOR-EACH loop ...")
    cursor2 = await con1.execute(query)
    async for rec in cursor2.records():
        print(rec)

    print("fetchmany 1 - user defined size ...")
    cursor3 = await con1.execute(query)
    records = await cursor3.fetchmany(30)
    for rec in records:
        print(rec)

    print("fetchmany 2 ...")
    cursor4 = await con1.execute(query)
    records = await cursor4.fetchmany()
    for rec in records:
        print(rec)

    await con1.close()



async def main():
    print(f'{time.ctime()} Hello!')
    await example_async()
    print(f'{time.ctime()} Goodbye!')


asyncio.run(main())
