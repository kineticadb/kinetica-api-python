from __future__ import print_function

import logging
import sys
import argparse
import datetime
import random
import threading
import time

try:
    import queue
    from queue import Queue
except ImportError:
    import Queue as queue
    from queue import Queue

if sys.version_info[0] == 2:
    from gpudb import GPUdbColumnProperty as GCP, GPUdbRecordColumn as GRC
    from gpudb import TableEventType, TableEvent, \
        NotificationEventType, NotificationEvent, GPUdbTableMonitorBase
    from gpudb import gpudb
else:
    from gpudb import GPUdbColumnProperty as GCP, GPUdbRecordColumn as GRC
    from gpudb.gpudb_table_monitor import TableEventType, TableEvent, \
        NotificationEventType, NotificationEvent, GPUdbTableMonitorBase
    from gpudb import gpudb


"""
This example demonstrates the usage of the QueuedGPUdbTableMonitor class
which is provided as a default implementation of the GPUdbTableMonitor class.

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

class QueuedGPUdbTableMonitor(GPUdbTableMonitorBase):
    """ A default implementation which just passes on the received objects
        to a simple Queue which is passed in as an argument to the constructor
        of this class.

        This class can be used as it is for simple requirements or more
        involved cases could directly inherit from GPUdbTableMonitor class and
        implement the callbacks to do further downstream processing.

    """

    def __init__(self, db, tablename,
                 record_queue, options = None):
        """ Constructor for QueuedGPUdbTableMonitor class

        Args:
            db (GPUdb):
                The handle to the GPUdb
            
            tablename (str):
                Name of the table to create the monitor for
            
            record_queue (queue.Queue):
                A Queue instance where notifications along with payloads can be
                passed into for client to consume and act upon
            
            options (GPUdbTableMonitor.Options):
                Options instance which is passed on to the super class
                GPUdbTableMonitor constructor
        """
        callbacks = GPUdbTableMonitorBase.Callbacks(cb_insert_raw=self.on_insert_raw,
                                                    cb_insert_decoded=self.on_insert_decoded,
                                                    cb_updated=self.on_update,
                                                    cb_deleted=self.on_delete,
                                                    cb_table_dropped=self.on_table_dropped
                                                    )
        super(QueuedGPUdbTableMonitor, self).__init__(
            db, tablename,
            callbacks, options=options)

        self.record_queue = record_queue

    def on_insert_raw(self, payload):
        """

        Args:
            payload:
        """
        self._logger.info("Payload received : %s " % payload)
        table_event = TableEvent(TableEventType.INSERT,
                                 count=-1, record_list=list(payload))
        self.record_queue.put(table_event)

    def on_insert_decoded(self, payload):
        """

        Args:
            payload:
        """
        self._logger.info("Payload received : %s " % payload)
        table_event = TableEvent(TableEventType.INSERT,
                                 count=-1, record_list=payload)
        self.record_queue.put(table_event)

    def on_update(self, count):
        """

        Args:
            count:
        """
        self._logger.info("Update count : %s " % count)
        table_event = TableEvent(TableEventType.UPDATE, count=count)
        self.record_queue.put(table_event)

    def on_delete(self, count):
        """

        Args:
            count:
        """
        self._logger.info("Delete count : %s " % count)
        table_event = TableEvent(TableEventType.DELETE, count=count)
        self.record_queue.put(table_event)

    def on_table_dropped(self, table_name):
        """
        Args:
            table_name:

        """
        self._logger.error("Table %s dropped " % self.table_name)
        notif_event = NotificationEvent(NotificationEventType.TABLE_DROPPED,
                                        table_name)
        self.record_queue.put(notif_event)


class TableMonitorExampleClient(threading.Thread):

    def __init__(self, table_monitor, work_queue):
        """
        [summary]

        Args:
            table_monitor (GPUdbTableMonitor): An instance of 
                GPUdbTableMonitor class
            
            work_queue (Queue): A Queue instance shared by this client and
                the GPUdbTableMonitor subclass for doing the notification
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
                if isinstance(item, TableEvent) \
                        and item.table_event_type == TableEventType.INSERT \
                        and item.records is not None \
                        and len(item.records) > 0:
                    for index, message in enumerate(item.records):
                        print(message)
                elif isinstance(item, TableEvent) \
                        and (item.table_event_type in [TableEventType.DELETE, TableEventType.UPDATE]) \
                        and item.count is not None \
                        and item.count > 0:
                    if item.table_event_type == TableEventType.DELETE:
                        print("Records deleted = %s" % item.count)
                    else:
                        print("Records updated = %s" % item.count)

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


def load_data():
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
    history_table = gpudb.GPUdbTable(name="table_monitor_history", db=h_db)

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


def create_tables():
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

    # Create a column list for the "status" table
    columns = [
        ["city", GRC._ColumnType.STRING, GCP.CHAR16, GCP.PRIMARY_KEY],
        ["state_province", GRC._ColumnType.STRING, GCP.CHAR32, GCP.PRIMARY_KEY],
        ["country", GRC._ColumnType.STRING, GCP.CHAR16],
        ["temperature", GRC._ColumnType.DOUBLE],
        ["last_update_ts", GRC._ColumnType.STRING, GCP.DATETIME]
    ]

    # Create the "status" table using the column list
    gpudb.GPUdbTable(
        columns,
        name="table_monitor_status",
        options=schema_option,
        db=h_db
    )


# end create_tables()


""" Drop the city weather "history" & "status" tables used in this example
"""


def clear_tables():
    # Drop all the tables
    for table_name in reversed([
        "table_monitor_status",
        "table_monitor_history"
    ]):
        h_db.clear_table(table_name)


# end clear_tables()

def delete_records(h_db):
    """

    Args:
        h_db:

    Returns:

    """
    print("In delete records ...")
    history_table = gpudb.GPUdbTable(name="table_monitor_history", db=h_db)
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
    parser.add_argument('--username', default='admin', help='Username of user to run example with')
    parser.add_argument('--password', default='Kinetica1!', help='Password of '
                                                              'user')
    parser.add_argument('--tablename', default='table_monitor_history', help='Name of Kinetica table to monitor')

    args = parser.parse_args()

    # Establish connection with an instance of Kinetica on port 9191
    h_db = gpudb.GPUdb(encoding="BINARY", host=args.host, port="9191", 
                       username=args.username, password=args.password)
    
    # Identify the message queue, running on port 9002
    table_monitor_queue_url = "tcp://" + args.host + ":9002"
    tablename = args.tablename

    # If command line arg is clear, just clear tables and exit
    if (args.command == "clear"):
        clear_tables()
        quit()

    clear_tables()

    create_tables()

    # This is the main client code
    # First create a Queue, create a TableMonitor object and call the
    # 'start_monitor' method
    # operation_list = [TableEventType.INSERT, TableEventType.DELETE]
    # notification_list = [NotificationEventType.TABLE_ALTERED,
    #                      NotificationEventType.TABLE_DROPPED]
    work_queue = Queue()
    monitor = QueuedGPUdbTableMonitor(h_db, tablename,
                                      record_queue=work_queue)
    monitor.logging_level = logging.DEBUG

    client = TableMonitorExampleClient(table_monitor=monitor,
                                       work_queue=work_queue)

    client.start()
    monitor.start_monitor()

    load_data()

    delete_records(h_db)
    # Create the client object, passing in the Queue object created earlier
    # and passed to the TableMonitor ctor.
    # The TableMonitorClient class demonstrates a possible usage of the
    # TableMonitor class
    time.sleep(10)
    client.close()

    # Stop the Table monitor after the client is done with it
    monitor.stop_monitor()
