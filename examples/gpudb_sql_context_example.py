import argparse
import os
import ssl
import uuid

from gpudb import GPUdb, GPUdbSqlContext, GPUdbTableClause, GPUdbSamplesClause


class GPUdbSqlContextExample(object):

    @staticmethod
    def create_context(url, username, password):
        # We want to have our own individual context to work with, so create an extension to the context name as shown here
        extension: str = str(uuid.uuid4()).replace('-', '_')

        # Set the SQL context to use
        kinetica_ctx: str = f'nyse_vector_ctxt_{extension}'
        # create the Kinetica connection
        if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
                getattr(ssl, '_create_unverified_context', None)):
            ssl._create_default_https_context = ssl._create_unverified_context

        options = GPUdb.Options()
        options.username = username
        options.password = password

        kdbc: GPUdb = GPUdb(host=url, options=options)

        table_ctx = GPUdbTableClause(
            table="nyct2020",
            comment="This table contains spatial boundaries and attributes of the New York City.",
            col_comments=dict(
                gid="This is the unique identifier for each record in the table.",
                geom="The spatial boundary in WKT format of each NTA neighborhood.",
                BoroCode="The code of the borough to which the neighborhood belongs to."),
            rules=["Join this table using STXY_WITHIN() = 1",
                   "Another rule here"])

        samples_ctx = GPUdbSamplesClause(samples=[
            ("What are the shortest, average, and longest trip lengths for each taxi vendor?",
             """
             SELECT th.vendor_id,
                 MIN(th.trip_distance) AS shortest_trip_length,
                 AVG(th.h.trip_distance) AS average_trip_length,
                 MAX(th.trip_distance) AS longest_trip_length
             FROM sa_quickstart.taxi_data_historical AS th
             GROUP BY th.vendor_id;
             """),

            ("How many trips did each taxi vendor make to JFK International Airport?",
             """
             SELECT th.vendor_id,
                 COUNT(*) AS trip_count
             FROM sa_quickstart.taxi_data_historical AS th
             JOIN sa_quickstart.nyct2020 AS n_dropoff ON KI_FN.STXY_WITHIN(th.dropoff_longitude, th.dropoff_latitude, n_dropoff.geom)
             AND n_dropoff.NTAName = 'John F. Kennedy International Airport'
             GROUP BY th.vendor_id;
             """),
        ])

        context_sql = GPUdbSqlContext(
            name="nyc_ctx",
            tables=[table_ctx],
            samples=samples_ctx).build_sql()

        print(context_sql)


if __name__ == '__main__':

    # Set up args
    parser = argparse.ArgumentParser(description='Run SQL context example.')
    parser.add_argument('--url', default='https://localhost:9191', help='Kinetica URL to run example against')
    parser.add_argument('--username', default=None, help='Username of user to run example with')
    parser.add_argument('--password', default=None, help='Password of user')

    args = parser.parse_args()

    GPUdbSqlContextExample.create_context(args.url, args.username, args.password)
