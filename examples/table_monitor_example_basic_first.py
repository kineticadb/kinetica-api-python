from __future__ import print_function

import argparse
import datetime
import logging
import random
import sys
import time

try:
    import queue
    from queue import Queue
except ImportError:
    import Queue as queue
    from queue import Queue

if sys.version_info[0] == 2:
    from gpudb import GPUdbColumnProperty as GCP, GPUdbRecordColumn as GRC
    from gpudb.gpudb_table_monitor import TableEventType, TableEvent, \
        NotificationEventType, GPUdbTableMonitorBase, NotificationEvent
    from gpudb import gpudb
else:
    from gpudb import GPUdbColumnProperty as GCP, GPUdbRecordColumn as GRC
    from gpudb import TableEventType, TableEvent, \
        NotificationEventType, GPUdbTableMonitorBase, NotificationEvent
    from gpudb import gpudb


# -----------------------------------------------------------------
#                          LOGGERS
# -----------------------------------------------------------------
# Module Level Loggers
logging.basicConfig(level=logging.DEBUG)
EXAMPLE_LOGGER = logging.getLogger(
    "table_monitor_example_basic_first.TableMonitorExampleClient")

BASETASK_LOGGER = logging.getLogger(
    "gpudb_table_monitor.BaseTask")

# Handlers need to be instantiated only ONCE for a given module
# (i.e. not per class instance)
handler1 = logging.StreamHandler()
formatter1 = logging.Formatter("%(asctime)s %(levelname)-8s {%("
                               "funcName)s:%(lineno)d} %(message)s",
                               "%Y-%m-%d %H:%M:%S")
handler1.setFormatter(formatter1)

EXAMPLE_LOGGER.addHandler(handler1)
BASETASK_LOGGER.addHandler(handler1)

# Prevent logging statements from being duplicated
EXAMPLE_LOGGER.propagate = False
BASETASK_LOGGER.propagate = False

"""
This example demonstrates the use of GPUdbTableMonitorBase class from an
example client class (TableMonitorExampleClient) where the instance of
GPUdbTableMonitorBase is used by composition.

The TableMonitorExampleClient class contains an instance of the
GPUdbTableMonitorBase class.

The main program runs as follows:
    # This is the main client code
    # First create a TableMonitorExampleClient instance and call the 'start'
    # method

1.   client = TableMonitorExampleClient(h_db, tablename)

    # Start the monitor
2.  client.start()

    # insert some data
3.  load_data()

    # delete a few records
4.  delete_records(h_db)

    # Give some time to the monitor to run
5.  time.sleep(10)

    # Finally stop the monitor
6.  client.stop()

"""


class TableMonitorExampleClient():

    def __init__(self, db, tablename, options=None):
        """ Constructor for TableMonitorExampleClient class.

        Args:
            db (GPUdb):
                The handle to the GPUdb
            
            tablename (str):
                Name of the table to create the monitor for
            
            options (GPUdbTableMonitorBase.Options):
                Options instance which is passed on to the super class
                GPUdbTableMonitorBase constructor
        """

        self.logger = EXAMPLE_LOGGER
        self.db = db
        self.table_name = tablename

        self.callbacks = GPUdbTableMonitorBase.Callbacks(
                                        cb_insert_raw=self.on_insert_raw,
                                        cb_insert_decoded=self.on_insert_decoded,
                                        cb_updated=self.on_update,
                                        cb_deleted=self.on_delete,
                                        cb_table_dropped=self.on_table_dropped
                                        )
        self.table_monitor = GPUdbTableMonitorBase(db, tablename, self.callbacks, options)
        self.table_monitor.logging_level = logging.DEBUG

    def start(self):
        self.table_monitor.start_monitor()

    def stop(self):
        self.table_monitor.stop_monitor()

    def __process_table_event(self, item):
        """
        This is an example of a method which is invoked from the callback
        methods with the respective payloads of TableEvent type.

        Similar approach could be taken in handling scenarios where the inputs
        from the callbacks could be used for further processing specific to
        different usage scenarios, e.g., ML, data processing pipelines etc.

        This method handles events pertaining to operations that can be done
        on Kinetica tables (insert, update, delete) and handle the changes
        received from the server side table monitors.

        This example implementation just prints the payloads but in reality
        they could be passed on to other processing methods, sent to a queue,
        used in ML pipelines etc.

        Args:
            item (TableEvent): The events could be one of TableEvent enum types
            like TableEventType.INSERT, TableEventType.UPDATE etc. The item is 
            checked for the exact type and could be handled differently as has
            been demonstrated in this method.
        """
        if item is not None:
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

    def __process_notification_events(self, notification_event):
        """
        This is an example of a method which is invoked from the callback
        methods with the respective payloads of NotificationEvent type.

        This example implementation just prints the payloads but in reality
        they could be passed on to other processing methods, logged for error 
        and system health tracing etc.

        Args:
            notification_event (NotificationEvent): The events could be one of 
            NotificationEvent enum types like NotificationEvent.TABLE_ALTERED, 
            NotificationEvent.TABLE_DROPPED etc. The item is checked for the 
            exact type and could be handled differently as has been demonstrated
            in this method.
        """
        if notification_event is not None:
            if isinstance(notification_event, NotificationEvent):
                print(notification_event.message)

    def on_insert_raw(self, payload):
        """

        Args:
            payload:
        """
        self.logger.info("Payload received : %s " % payload)
        table_event = TableEvent(TableEventType.INSERT,
                                 count=-1, record_list=list(payload))
        self.__process_table_event(table_event)

    def on_insert_decoded(self, payload):
        """

        Args:
            payload:
        """
        self.logger.info("Payload received : %s " % payload)
        table_event = TableEvent(TableEventType.INSERT,
                                 count=-1, record_list=payload)
        self.__process_table_event(table_event)

    def on_update(self, count):
        """

        Args:
            count:
        """
        self.logger.info("Update count : %s " % count)
        table_event = TableEvent(TableEventType.UPDATE, count=count)
        self.__process_table_event(table_event)

    def on_delete(self, count):
        """

        Args:
            count:
        """
        self.logger.info("Delete count : %s " % count)
        table_event = TableEvent(TableEventType.DELETE, count=count)
        self.__process_table_event(table_event)

    def on_table_dropped(self, table_name):
        """
        Args:
            table_name:

        """
        self.logger.error("Table %s dropped " % self.table_name)
        notif_event = NotificationEvent(NotificationEventType.TABLE_DROPPED,
                                        table_name)
        self.__process_notification_events(notif_event)

# End class TableMonitorExampleClient


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
    history_table = gpudb.GPUdbTable(name="examples.table_monitor_history", db=h_db)

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
        "examples.table_monitor_status",
        "examples.table_monitor_history"
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
    history_table = gpudb.GPUdbTable(name="examples.table_monitor_history", db=h_db)
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
    parser.add_argument('--host', default='10.0.0.21', help='Kinetica host to '
                                                            'run '
                                                            'example against')
    parser.add_argument('--port', default='9191', help='Kinetica port')
    parser.add_argument('--username', default='admin', help='Username of user to run example with')
    parser.add_argument('--password', default='Kinetica1!', help='Password of '
                                                              'user')
    parser.add_argument('--tablename', default='examples.table_monitor_history', help='Name of Kinetica table to monitor')

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
    # First create a TableMonitorExampleClient instance and call the 'start'
    # method

    client = TableMonitorExampleClient(h_db, tablename)

    # Start the monitor
    client.start()

    # insert some data
    load_data()

    # delete a few records
    delete_records(h_db)

    # Give some time to the monitor to run
    time.sleep(10)

    # Finally stop the monitor
    client.stop()
