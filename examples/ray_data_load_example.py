import os
import argparse
import ray
from gpudb.dbapi import *


URL = os.getenv('KINETICA_URL', 'http://localhost:9191')
USER = os.getenv('USERNAME', "")
PASS = os.getenv('PASSWORD', "")
SCHEMA = os.getenv('PY_TEST_SCHEMA', 'example_ray')
BYPASS_SSL_CERT_CHECK = os.getenv('PY_TEST_BYPASS_CERT_CHECK', True)
if BYPASS_SSL_CERT_CHECK in ["1", 1]:
    BYPASS_SSL_CERT_CHECK = True
    
CONN = None

def create_connection(url = None, username = None, password = None):
    global CONN
    
    if CONN:
        print(f"Using existing connection to : {CONN.connection.get_url()}")
    else:
        print(f"Creating connection to : {url}")
        CONN = connect(
            "kinetica://",
            url = url,
            username = username,
            password = password,
            options={
                "skip_ssl_cert_verification": BYPASS_SSL_CERT_CHECK,
            }
        )
    
    return CONN

def example_ray_data_load(url, username, password, schema):
    """ Pre-requisite - pip install -U "ray[data,train,tune,serve] - to test locally with ray """

    global CONN
    
    CONN = create_connection(url, username, password)

    table_name = f'"{schema}"."ray_data_load_example"'

    print(f"Creating table : {table_name}")

    create_table = f"""
    CREATE OR REPLACE TABLE {table_name}
    (
        "i" INTEGER NOT NULL,
        "d" DOUBLE NOT NULL,
        "s" VARCHAR NOT NULL,
        "f" REAL NOT NULL,
        "l" BIGINT NOT NULL
    )
    """

    CONN.execute(create_table)
    # ParamStyle - numeric_dollar
    insert_stmt = "INSERT INTO {} (i, d, s, f, l) VALUES ($1, $2, $3, $4, $5)".format(
        table_name
    )
    print(f"Single insert statement : {insert_stmt}")
    CONN.execute(insert_stmt, [1, 144444444.4, "s1", 234.5, 1000000000])

    data_query = f"SELECT * FROM {table_name}"
    print(f"Data query : {data_query}")
    dataset = ray.data.read_sql(data_query, create_connection)

    print(f"Dataset count : {dataset.count()}")
    print("Dataset record(s) :")
    for row in dataset.iter_rows():
        print(f"* {row}")


if __name__=="__main__":

    # Set up args
    parser = argparse.ArgumentParser(description='Run ray example.')
    parser.add_argument('--url', default=URL, help='Kinetica URL to run example against')
    parser.add_argument('--username', default=USER, help='Username of user to run example with')
    parser.add_argument('--password', default=PASS, help='Password of user')
    parser.add_argument('--schema', default=SCHEMA, help='Schema containing test tables')

    args = parser.parse_args()

    example_ray_data_load(args.url, args.username, args.password, args.schema)
