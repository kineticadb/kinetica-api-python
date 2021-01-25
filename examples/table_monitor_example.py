from __future__ import print_function

import argparse
import datetime
import logging
import random
import time

import gpudb
from gpudb import GPUdbColumnProperty as GCP, GPUdbRecordColumn as GRC, \
    GPUdbTableMonitor


"""
This example demonstrates the usage of the GPUdbTableMonitor.Client class.
The class GPUdbTableMonitorExample derives from the class
GPUdbTableMonitor.Client and defines the callbacks. It is the list of these
callback objects that is passed to the constructor of the Client class.

This example shows a simple way of extending the GPUdbTableMonitor.Client class
to get notifications about data from the table monitors.

The main methods runs as follows:

    # Create a GPUdbTableMonitorExample class
1.  monitor = GPUdbTableMonitorExample(h_db, tablename)

    # Start the monitor
2.  monitor.start_monitor()

    # Load some data
3.  load_data()

    # Delete some records
4.  delete_records(h_db)

    # Wait for some time and let the monitor work
5.  time.sleep(5)

    # Stop the Table monitor after the client is done with it
6.  monitor.stop_monitor()


"""




""" Load random city weather data into a "history" table, in batches.  Each
    batch will be loaded 2 seconds apart, to give the table monitor time to push
    that batch to the message queue and the queue client time to process the
    batch
"""


def load_data(table_name):
    # Base data set, from which cities will be randomly chosen, with a random
    #   new temperature picked for each, per batch loaded
    city_data = [
        ["Washington", "DC", "USA", -77.016389, 38.904722, 58.5, "UTC-5"],
        ["Paris", "TX", "USA", -95.547778, 33.6625, 64.6, "UTC-6"],
        ["Memphis", "TN", "USA", -89.971111, 35.1175, 63, "UTC-6"],
        ["Sydney", "Nova Scotia", "Canada", -60.19551, 46.13631, 44.5, "UTC-4"],
        ["La Paz", "Baja California Sur", "Mexico", -110.310833, 24.142222, 77, "UTC-7"],
        ["St. Petersburg", "FL", "USA", -82.64, 27.773056, 74.5, "UTC-5"],
        ["Oslo", "--", "Norway", 10.75, 59.95, 45.5, "UTC+1"],
        ["Paris", "--", "France", 2.3508, 48.8567, 56.5, "UTC+1"],
        ["Memphis", "--", "Egypt", 31.250833, 29.844722, 73, "UTC+2"],
        ["St. Petersburg", "--", "Russia", 30.3, 59.95, 43.5, "UTC+3"],
        ["Lagos", "Lagos", "Nigeria", 3.384082, 6.455027, 83, "UTC+1"],
        ["La Paz", "Pedro Domingo Murillo", "Bolivia", -68.15, -16.5, 44, "UTC-4"],
        ["Sao Paulo", "Sao Paulo", "Brazil", -46.633333, -23.55, 69.5, "UTC-3"],
        ["Santiago", "Santiago Province", "Chile", -70.666667, -33.45, 62, "UTC-4"],
        ["Buenos Aires", "--", "Argentina", -58.381667, -34.603333, 65, "UTC-3"],
        ["Manaus", "Amazonas", "Brazil", -60.016667, -3.1, 83.5, "UTC-4"],
        ["Sydney", "New South Wales", "Australia", 151.209444, -33.865, 63.5, "UTC+10"],
        ["Auckland", "--", "New Zealand", 174.74, -36.840556, 60.5, "UTC+12"],
        ["Jakarta", "--", "Indonesia", 106.816667, -6.2, 83, "UTC+7"],
        ["Hobart", "--", "Tasmania", 147.325, -42.880556, 56, "UTC+10"],
        ["Perth", "Western Australia", "Australia", 115.858889, -31.952222, 68, "UTC+8"]
    ]

    # Grab a handle to the history table for inserting new weather records
    history_table = gpudb.GPUdbTable(name=table_name, db=h_db)

    random.seed(0)

    # Insert 5 batches of city weather records
    # ========================================

    for iter in range(5):

        city_updates = []

        # Grab a random set of cities
        cities = random.sample(city_data, k=random.randint(1, int(len(city_data) / 2)))

        # Create a list of weather records to insert
        for city in cities:
            # Pick a random temperature for each city at the current time
            city_update = list(city)
            city_update[5] = city_update[5] + random.randrange(-10, 10)
            city_update.append(datetime.datetime.now())

            city_updates.append(city_update)

        # Insert the records into the table and allow time for table monitor to
        #   process them before inserting the next batch
        print
        print("[Main/Loader]  Inserting <%s> new city temperatures..." % len(city_updates))
        history_table.insert_records(city_updates)

        time.sleep(2)


# end load_data_and_wait()


""" Create the city weather "history" & "status" tables used in this example
"""


def create_table(table_name):
    # Put both tables into the "examples" schema
    schema_option = {"collection_name": "examples"}

    # Create a column list for the "history" table
    columns = [
        ["city", GRC._ColumnType.STRING, GCP.CHAR16],
        ["state_province", GRC._ColumnType.STRING, GCP.CHAR32],
        ["country", GRC._ColumnType.STRING, GCP.CHAR16],
        ["x", GRC._ColumnType.DOUBLE],
        ["y", GRC._ColumnType.DOUBLE],
        ["temperature", GRC._ColumnType.DOUBLE],
        ["time_zone", GRC._ColumnType.STRING, GCP.CHAR8],
        ["ts", GRC._ColumnType.STRING, GCP.DATETIME]
    ]

    # Create the "history" table using the column list
    gpudb.GPUdbTable(
        columns,
        name="table_monitor_history",
        options=schema_option,
        db=h_db
    )



# end create_tables()


""" Drop the city weather "history" table used in this example
"""


def clear_table(table_name):
    # Drop all the tables
    h_db.clear_table(table_name)


# end clear_tables()

def delete_records(h_db, table_name):
    """

    Args:
        h_db (GPUdb): GPUdb object
        table_name (str): Name of the table.

    Returns: Number of records deleted.

    """
    print("In delete records ...")
    history_table = gpudb.GPUdbTable(name=table_name, db=h_db)
    pre_delete_records = history_table.size()
    print("Records before = %s" % pre_delete_records)
    delete_expr = ["state_province = 'Sao Paulo'"]
    history_table.delete_records(expressions=delete_expr)
    post_delete_records = history_table.size()
    print("Records after = %s" % post_delete_records)

    return pre_delete_records - post_delete_records


class GPUdbTableMonitorExample(GPUdbTableMonitor.Client):
    """ An example implementation which just logs the table monitor events in the
        call back methods which are defined..

        This class can be used as it is for simple requirements or more
        involved cases as well where the callback could be used for more complex
        processing instead of just logging the payloads.
    """

    def __init__(self, db, table_name, options=None):
        """ Constructor for GPUdbTableMonitor class

        Args:
            db (GPUdb):
                The handle to the GPUdb

            table_name (str):
                Name of the table to create the monitor for

            options (GPUdbTableMonitor.Options):
                Options instance which is passed on to the super class
                GPUdbTableMonitor.Client constructor
        """

        # Create the list of callbacks objects which are to be passed to the
        # 'GPUdbTableMonitor.Client' class constructor
        callbacks = [
            GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.INSERT_RAW,
                                      self.on_insert_raw,
                                      self.on_error),

            GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.INSERT_DECODED,
                                      self.on_insert_decoded,
                                      self.on_error,
                                      GPUdbTableMonitor.Callback.InsertDecodedOptions( GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.ABORT )),

            GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.UPDATED,
                                      self.on_update,
                                      self.on_error),

            GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.DELETED,
                                      self.on_delete,
                                      self.on_error),

            GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.TABLE_DROPPED,
                                      self.on_table_dropped,
                                      self.on_error),

            GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.TABLE_ALTERED,
                                      self.on_table_altered,
                                      self.on_error)
        ]

        # Invoke the base class constructor and pass in the list of callback
        # objects created earlier.  This invocation is mandatory for the table
        # monitor to be actually functional.
        super(GPUdbTableMonitorExample, self).__init__(db, table_name,
                                                       callback_list=callbacks,
                                                       options=options)

    def on_insert_raw(self, record):
        """Callback method which is invoked with the raw payload bytes
           received from the table monitor when a new record is inserted

           This callback method is needed for an 'insert' table monitor to be
           created. Not passing this method or 'on_insert_decoded' will be
           like declaring that an 'insert' monitor is not needed and that the
           user is not interested in getting notifications about insertions to
           the concerned table.

        Args:
            record (bytes): This is a collection of undecoded bytes. Decoding
            is left to the user who uses this callback.
        """
        # Override the method
        # Call the base class method , just for example

        self._logger.info("Raw payload received is : %s " % record)


    def on_insert_decoded(self, record):
        """Callback method which is invoked with the decoded payload record
           received from the table monitor when a new record is inserted

           This callback method is needed for an 'insert' table monitor to be
           created. Not passing this method or 'on_insert_raw' will be
           like declaring that an 'insert' monitor is not needed and that the
           user is not interested in getting notifications about insertions to
           the concerned table.

        Args:
            record (dict): This will be a dict in the format given below
            {u'state_province': u'--', u'city': u'Auckland',
            u'temperature': 57.5, u'country': u'New Zealand',
            u'time_zone': u'UTC+12',
            u'ts': u'2020-09-28 00:28:37.481119', u'y': -36.840556,
            u'x': 174.74}
        """
        # Override the method
        # Call the base class method , just for example

        self._logger.info("Decoded payload received is : %s " % record)


    def on_update(self, count):
        """Callback method which is invoked with the number of records updated
           as received from the table monitor when records are updated

        Args:
            count (int): This is the actual number of records updated.
        """
        self._logger.info("Update count : %s " % count)

    def on_delete(self, count):
        """Callback method which is invoked with the number of records updated
           as received from the table monitor when records are deleted

        Args:
            count (int): This is the actual number of records deleted.
        """
        self._logger.info("Delete count : %s " % count)

    def on_table_dropped(self, table_name):
        """Callback method which is invoked with the name of the table which
           is dropped when the table monitor is in operation.
        Args:
            table_name (str): Name of the table dropped
        """
        self._logger.error("Table %s dropped " % table_name)

    def on_table_altered(self, table_name):
        """Callback method which is invoked with the name of the table which
           is altered when the table monitor is in operation.
        Args:
            table_name (str): Name of the table altered
        """
        self._logger.error("Table %s altered " % table_name)

    def on_error(self, message):
        """Callback method which is invoked with the error message
           when some error has occurred.
        Args:
            message (str): The error message; often wrapping an exception
            raised.
        """
        self._logger.error("Error occurred " % message)


# End GPUdbTableMonitorExample class

if __name__ == '__main__':
    # Set up args.. see
    parser = argparse.ArgumentParser(description='Run table monitor example.')
    parser.add_argument('command', nargs="?",
                        help='command to execute (currently only "clear" to remove the example tables')
    parser.add_argument('--host', default='localhost', help='Kinetica host to '
                                                            'run '
                                                            'example against')
    parser.add_argument('--port', default='9191', help='Kinetica port')
    parser.add_argument('--username', help='Username of user to run example with')
    parser.add_argument('--password', help='Password of user')

    args = parser.parse_args()

    # Establish connection with an instance of Kinetica on port 9191
    h_db = gpudb.GPUdb(encoding="BINARY", host=args.host, port="9191", 
                       username=args.username, password=args.password)
    
    # Identify the message queue, running on port 9002
    table_monitor_queue_url = "tcp://" + args.host + ":9002"
    tablename = 'examples.table_monitor_history'

    # If command line arg is clear, just clear tables and exit
    if (args.command == "clear"):
        clear_table(tablename)
        quit()

    clear_table(tablename)

    create_table(tablename)

    # This is the main client code

    # Create a GPUdbTableMonitor class
    monitor = GPUdbTableMonitorExample( h_db, tablename )
    monitor.logging_level = logging.DEBUG

    # Start the monitor
    monitor.start_monitor()

    # Load some data
    load_data(tablename)

    # Delete some records
    delete_records(h_db, tablename)

    # Wait for some time and let the monitor work
    time.sleep(5)

    # Stop the Table monitor after the client is done with it
    monitor.stop_monitor()
