import argparse
import os
from gpudb.dbapi import *


URL = os.getenv('PY_TEST_URL', 'http://localhost:9191')
USER = os.getenv('PY_TEST_USER', "")
PASS = os.getenv('PY_TEST_PASS', "")
SCHEMA = os.getenv('PY_TEST_SCHEMA', 'example_dbapi_sync')
BYPASS_SSL_CERT_CHECK = os.getenv('PY_TEST_BYPASS_CERT_CHECK', True)
if BYPASS_SSL_CERT_CHECK in ["1", 1]:
    BYPASS_SSL_CERT_CHECK = True


def example_sync(url, username, password, schema):
    """Sync calls"""

    con1 = connect(
        "kinetica://",
        url = url,
        username = username,
        password = password,
        default_schema = schema,
        options = {
            "skip_ssl_cert_verification": BYPASS_SSL_CERT_CHECK,
        },
    )

    table_name = f'"{SCHEMA}"."dbapi_example"' if SCHEMA else "dbapi_example"

    create_table = f"""
    CREATE TABLE {table_name}
    (
        "i" INTEGER NOT NULL,
        "d" DOUBLE NOT NULL,
        "s" VARCHAR NOT NULL,
        "f" REAL NOT NULL,
        "l" BIGINT NOT NULL
    ) USING TABLE PROPERTIES (no_error_if_exists = TRUE)"""

    con1.execute(create_table)
    # ParamStyle - numeric_dollar
    insert_query = "insert into {} (i, d, s, f, l) values ($1, $2, $3, $4, $5)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])

    # ParamStyle - qmark
    insert_query = "insert into {} (i, d, s, f, l) values (?, ?, ?, ?, ?)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])

    # ParamStyle - numeric
    insert_query = "insert into {} (i, d, s, f, l) values (:1, :2, :3, :4, :5)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])

    # ParamStyle - format
    insert_query = "insert into {} (i, d, s, f, l) values (%i, %f, %s, %f, %l)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])

    # ParamStyle - mixed/erroneous
    insert_query = "insert into {} (i, d, s, f, l) values (%i, $2, :3, ?, %l)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    try:
        con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])
    except ProgrammingError as e:
        print(e)

    print("QUERY ...")
    query = "select * from {}".format(table_name)
    cursor1 = con1.execute(query)
    print(cursor1.description)

    print("WHILE loop ...")
    record = cursor1.fetchone()
    while record:
        print(record)
        record = cursor1.fetchone()

    print("FOR-EACH loop ...")
    cursor2 = con1.execute(query)
    for rec in cursor2:
        print(rec)

    print("fetchmany 1 - user defined size ...")
    cursor3 = con1.execute(query)
    records = cursor3.fetchmany(30)
    for rec in records:
        print(rec)

    print("fetchmany 2 ...")
    cursor4 = con1.execute(query)
    records = cursor4.fetchmany()
    for rec in records:
        print(rec)

    con1.close()



if __name__ == "__main__":

    # Set up args
    parser = argparse.ArgumentParser(description='Run synchronous DBAPI example.')
    parser.add_argument('--url', default=URL, help='Kinetica URL to run example against')
    parser.add_argument('--username', default=USER, help='Username of user to run example with')
    parser.add_argument('--password', default=PASS, help='Password of user')
    parser.add_argument('--schema', default=SCHEMA, help='Schema containing test tables')

    args = parser.parse_args()

    example_sync(args.url, args.username, args.password, args.schema)
