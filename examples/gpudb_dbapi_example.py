from gpudb.dbapi import *


if __name__ == '__main__':
    con1 = connect("kinetica://", connect_args={
        'url': 'http://localhost:9191',
        'username': '',
        'password': '',
        'bypass_ssl_cert_check': True})

    table_name = '"ki_home"."dbapi_example"'

    create_table = """
    CREATE TABLE "ki_home"."dbapi_example"
    (
        "i" INTEGER NOT NULL,
        "d" DOUBLE NOT NULL,
        "s" VARCHAR NOT NULL,
        "f" REAL NOT NULL,
        "l" BIGINT NOT NULL
    ) using table properties (no_error_if_exists=TRUE)"""

    con1.execute(create_table)
    # ParamStyle - numeric_dollar
    insert_query = "insert into {} (i, d, s, f, l) values ($1, $2, $3, $4, $5)".format(table_name)
    print("Single insert query : {}".format(insert_query))
    con1.execute(insert_query, [1, 144444444.4, 's1', 234.5, 1000000000])

    # ParamStyle - qmark
    insert_query = "insert into {} (i, d, s, f, l) values (?, ?, ?, ?, ?)".format(table_name)
    print("Single insert query : {}".format(insert_query))
    con1.execute(insert_query, [1, 144444444.4, 's1', 234.5, 1000000000])

    # ParamStyle - numeric
    insert_query = "insert into {} (i, d, s, f, l) values (:1, :2, :3, :4, :5)".format(table_name)
    print("Single insert query : {}".format(insert_query))
    con1.execute(insert_query, [1, 144444444.4, 's1', 234.5, 1000000000])

    # ParamStyle - format
    insert_query = "insert into {} (i, d, s, f, l) values (%i, %f, %s, %f, %l)".format(table_name)
    print("Single insert query : {}".format(insert_query))
    con1.execute(insert_query, [1, 144444444.4, 's1', 234.5, 1000000000])

    # ParamStyle - mixed/erroneous
    insert_query = "insert into {} (i, d, s, f, l) values (%i, $2, :3, ?, %l)".format(table_name)
    print("Single insert query : {}".format(insert_query))
    try:
        con1.execute(insert_query, [1, 144444444.4, 's1', 234.5, 1000000000])
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
