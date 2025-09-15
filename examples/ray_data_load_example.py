import os
import ray
from gpudb.dbapi import *


URL = os.getenv('KINETICA_URL', 'http://localhost:9191')
USER = os.getenv('USERNAME', "")
PASS = os.getenv('PASSWORD', "")
SCHEMA = os.getenv('PY_TEST_SCHEMA', 'ki_home')
BYPASS_SSL_CERT_CHECK = os.getenv('PY_TEST_BYPASS_CERT_CHECK', True)
if BYPASS_SSL_CERT_CHECK in ["1", 1]:
    BYPASS_SSL_CERT_CHECK = True

def create_connection():
    return  connect(
        "kinetica://",
        connect_args={
            "url": URL,
            "username": USER,
            "password": PASS,
            "bypass_ssl_cert_check": BYPASS_SSL_CERT_CHECK,
        },
    )

def example_ray_data_load():
    """ Pre-requisite - pip install -U "ray[data,train,tune,serve] - to test locally with ray """

    con1 = connect(
        "kinetica://",
        connect_args={
            "url": URL,
            "username": USER,
            "password": PASS,
            "bypass_ssl_cert_check": BYPASS_SSL_CERT_CHECK,
        },
    )

    table_name = f'"{SCHEMA}"."dbapi_example"'

    create_table = f"""
    CREATE TABLE {table_name}
    (
        "i" INTEGER NOT NULL,
        "d" DOUBLE NOT NULL,
        "s" VARCHAR NOT NULL,
        "f" REAL NOT NULL,
        "l" BIGINT NOT NULL
    ) using table properties (no_error_if_exists=TRUE)"""

    con1.execute(create_table)
    # ParamStyle - numeric_dollar
    insert_query = "insert into {} (i, d, s, f, l) values ($1, $2, $3, $4, $5)".format(
        table_name
    )
    print("Single insert query : {}".format(insert_query))
    con1.execute(insert_query, [1, 144444444.4, "s1", 234.5, 1000000000])

    dataset = ray.data.read_sql(f"SELECT * FROM {table_name}", create_connection)
    print(dataset.count)
    for row in dataset.iter_rows():
        print(row)


if __name__=="__main__":
    example_ray_data_load()
