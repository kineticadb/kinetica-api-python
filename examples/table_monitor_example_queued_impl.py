from __future__ import print_function

import argparse
import datetime
import logging
import random
import threading
import time

try:
    import queue
    from queue import Queue
except ImportError:
    import Queue as queue
    from queue import Queue

import gpudb
from gpudb import GPUdbColumnProperty as GCP, GPUdbRecordColumn as GRC, \
    GPUdbTableMonitor

"""
This example demonstrates a scenario where the GPUdbTableMonitor.Client class
might be needed to be used in a code which already runs in it's own thread.

Since the table monitor Client class itself runs threads internally, it is
possible to pass on the notification data received to the user code using a
shared Queue, which this example shows.

The class QueuedGPUdbTableMonitor derives from GPUdbTableMonitor.Client class
and defines the callback methods.

The class TableMonitorExampleClient is a class running in its own thread
and communicating with an instance of QueuedGPUdbTableMonitor class using
a Queue instance.

The main method does the following:
    1. Creates a Queue instance
    2. Creates an instance of TableMonitorExampleClient with the Queue instance
        created.
    3. Creates an instance of QueuedGPUdbTableMonitor class with the Queue
        instance.
    4. Starts the client.
    5. Starts the table monitor.
    6. Performs some table operations like inserts and deletes.
    7. The client class receives the notification data in the shared Queue
        and prints out the data received.

"""


class QueuedGPUdbTableMonitor(GPUdbTableMonitor.Client):
    """ An example implementation which just passes on the received objects
        to a simple Queue which is passed in as an argument to the constructor
        of this class.
    """

    def __init__(self, db, tablename,
                 record_queue, options=None):
        """ Constructor for QueuedGPUdbTableMonitor class

        Args:
            db (GPUdb):
                The handle to the GPUdb

            tablename (str):
                Name of the table to create the monitor for

            record_queue (queue.Queue):
                A Queue instance where notifications along with payloads can be
                passed into for client to consume and act upon

            options (GPUdbTableMonitor.Client.Options):
                Options instance which is passed on to the super class
                GPUdbTableMonitor constructor
        """
        # Define the callback methods and create the objects of type
        # GPUdbTableMonitor.Callback wrapping the callback methods according to
        # type of the callback object. Pass on the list of such objects to the
        # GPUdbTableMonitor.Client constructor to receive notifications of the
        # events of interest and implement custom processing of the payloads
        # received. The default behaviour only logs the payloads and does not
        # do anything more useful.

        # Create the list of callbacks objects which are to be passed to the
        # 'GPUdbTableMonitor.Client' class constructor
        callbacks = [
            GPUdbTableMonitor.Callback(
                GPUdbTableMonitor.Callback.Type.INSERT_RAW,
                self.on_insert_raw,
                self.on_error),

            GPUdbTableMonitor.Callback(
                GPUdbTableMonitor.Callback.Type.INSERT_DECODED,
                self.on_insert_decoded,
                self.on_error,
                GPUdbTableMonitor.Callback.InsertDecodedOptions(
                    GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.ABORT)),

            GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.UPDATED,
                                       self.on_update,
                                       self.on_error),

            GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.DELETED,
                                       self.on_delete,
                                       self.on_error),

            GPUdbTableMonitor.Callback(
                GPUdbTableMonitor.Callback.Type.TABLE_DROPPED,
                self.on_table_dropped,
                self.on_error),

            GPUdbTableMonitor.Callback(
                GPUdbTableMonitor.Callback.Type.TABLE_ALTERED,
                self.on_table_altered,
                self.on_error)
        ]

        # Invoke the base class constructor. This invocation is mandatory for
        # the table monitor to be actually functional.
        super(QueuedGPUdbTableMonitor, self).__init__(
            db, tablename, callback_list=callbacks,
            options=options)

        self.record_queue = record_queue

    def on_insert_raw(self, record):
        """Callback method which is invoked with the raw payload bytes
           received from the table monitor when a new record is inserted

        Args:
            record (bytes): This is a collection of undecoded bytes. Decoding
            is left to the user who uses this callback.
        """
        self._logger.info("Payload received : %s " % record)
        self.record_queue.put("Record inserted (raw) = %s" % record)

    def on_insert_decoded(self, record):
        """Callback method which is invoked with the decoded payload record
           received from the table monitor when a new record is inserted

        Args:
            record (dict): This will be a dict in the format given below
            {u'state_province': u'--', u'city': u'Auckland',
            u'temperature': 57.5, u'country': u'New Zealand',
            u'time_zone': u'UTC+12',
            u'ts': u'2020-09-28 00:28:37.481119', u'y': -36.840556,
            u'x': 174.74}
        """
        self._logger.info("Payload received : %s " % record)
        self.record_queue.put("Record inserted (decoded) = %s" % record)

    def on_update(self, count):
        """Callback method which is invoked with the number of records updated
           as received from the table monitor when records are updated

        Args:
            count (int): Number of records updated.
        """
        self._logger.info("Update count : %s " % count)
        self.record_queue.put("Update count : %s " % count)

    def on_delete(self, count):
        """Callback method which is invoked with the number of records updated
           as received from the table monitor when records are deleted

        Args:
            count (int): Number of records deleted.
        """
        self._logger.info("Delete count : %s " % count)
        self.record_queue.put("Delete count : %s " % count)

    def on_table_dropped(self, table_name):
        """Callback method which is invoked with the name of the table which
           is dropped when the table monitor is in operation.
        Args:
            table_name (str): Name of the table dropped.
        """
        self._logger.error("Table %s dropped " % table_name)
        self.record_queue.put("Table %s dropped " % table_name)

    def on_table_altered(self, message):
        """Callback method which is invoked with the name of the table which
           is altered when the table monitor is in operation.
        Args:
            message (str): Name of the table altered.
        """
        self._logger.error("Table %s altered " % message)
        self.record_queue.put("Table %s altered " % message)

    def on_error(self, message):
        """Callback method which is invoked with the error message
           when some error has occurred.
        Args:
            message: The error message; often wrapping an exception
            raised.
        """
        self._logger.error("Error occurred " % message)
        self.record_queue.put("Error occurred " % message)


class TableMonitorExampleClient(threading.Thread):

    def __init__(self, table_monitor, work_queue):
        """
        [summary]

        Args:
            table_monitor (GPUdbTableMonitor.Client): An instance of
                GPUdbTableMonitor.Client class or some derivative of it.

            work_queue (Queue): A Queue instance shared by this client and
                the GPUdbTableMonitor.Client subclass for doing the notification
                message exchange as they are received by the table monitor
                for various events (table operation related and otherwise)
        """

        super(TableMonitorExampleClient, self).__init__()
        self.table_monitor = table_monitor
        self.work_queue = work_queue
        self.kill = False

    def run(self):
        while not self.kill:
            print(self.kill)
            print("Looking for new items in queue ...")
            item = self.work_queue.get()  # timeout=1

            if item is None:
                break
            else:
                print(item)

        print("Exiting Client ...")

    def close(self):
        print("In close method ...")
        self.kill = True
        self.work_queue.put(None)
        self.join()


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
        ["La Paz", "Baja California Sur", "Mexico", -110.310833, 24.142222, 77,
         "UTC-7"],
        ["St. Petersburg", "FL", "USA", -82.64, 27.773056, 74.5, "UTC-5"],
        ["Oslo", "--", "Norway", 10.75, 59.95, 45.5, "UTC+1"],
        ["Paris", "--", "France", 2.3508, 48.8567, 56.5, "UTC+1"],
        ["Memphis", "--", "Egypt", 31.250833, 29.844722, 73, "UTC+2"],
        ["St. Petersburg", "--", "Russia", 30.3, 59.95, 43.5, "UTC+3"],
        ["Lagos", "Lagos", "Nigeria", 3.384082, 6.455027, 83, "UTC+1"],
        ["La Paz", "Pedro Domingo Murillo", "Bolivia", -68.15, -16.5, 44,
         "UTC-4"],
        ["Sao Paulo", "Sao Paulo", "Brazil", -46.633333, -23.55, 69.5, "UTC-3"],
        ["Santiago", "Santiago Province", "Chile", -70.666667, -33.45, 62,
         "UTC-4"],
        ["Buenos Aires", "--", "Argentina", -58.381667, -34.603333, 65,
         "UTC-3"],
        ["Manaus", "Amazonas", "Brazil", -60.016667, -3.1, 83.5, "UTC-4"],
        ["Sydney", "New South Wales", "Australia", 151.209444, -33.865, 63.5,
         "UTC+10"],
        ["Auckland", "--", "New Zealand", 174.74, -36.840556, 60.5, "UTC+12"],
        ["Jakarta", "--", "Indonesia", 106.816667, -6.2, 83, "UTC+7"],
        ["Hobart", "--", "Tasmania", 147.325, -42.880556, 56, "UTC+10"],
        ["Perth", "Western Australia", "Australia", 115.858889, -31.952222, 68,
         "UTC+8"]
    ]

    # Grab a handle to the history table for inserting new weather records
    history_table = gpudb.GPUdbTable(name=table_name, db=h_db)

    random.seed(0)

    # Insert 5 batches of city weather records
    # ========================================

    for iter in range(5):

        city_updates = []

        # Grab a random set of cities
        cities = random.sample(city_data,
                               k=random.randint(1, int(len(city_data) / 2)))

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
        print("[Main/Loader]  Inserting <%s> new city temperatures..." % len(
            city_updates))
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
        name=table_name,
        options=schema_option,
        db=h_db
    )


# end create_tables()


""" Drop the city weather "history" table
"""


def clear_table(table_name):
    # Drop all the tables
    h_db.clear_table(table_name)


# end clear_tables()

def delete_records(h_db, table_name):
    """

    Args:
        h_db:

    Returns:

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


if __name__ == '__main__':
    # Set up args
    parser = argparse.ArgumentParser(description='Run table monitor example.')
    parser.add_argument('command', nargs="?",
                        help='command to execute (currently only "clear" to remove the example tables')
    parser.add_argument('--host', default='localhost', help='Kinetica host to '
                                                            'run '
                                                            'example against')
    parser.add_argument('--port', default='9191', help='Kinetica port')
    parser.add_argument('--username',
                        help='Username of user to run example with')
    parser.add_argument('--password', help='Password of the given user')

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

    work_queue = Queue()

    # create the `QueuedGPUdbTableMonitor` class and pass in the Queue instance.
    monitor = QueuedGPUdbTableMonitor(h_db, tablename,
                                      record_queue=work_queue)
    monitor.logging_level = logging.DEBUG

    # Create the `TableMonitorExampleClient` class and pass in the Queue
    # instance.
    client = TableMonitorExampleClient(table_monitor=monitor,
                                       work_queue=work_queue)

    # Start the client
    client.start()

    # Start the table monitor
    monitor.start_monitor()

    load_data(tablename)

    delete_records(h_db, tablename)

    time.sleep(10)

    # Close the client
    client.close()

    # Stop the Table monitor after the client is done with it
    monitor.stop_monitor()
