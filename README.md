Kinetica Python API
===================

This is the 7.1.x.y version of the client-side Python API for Kinetica.  The
first two components of the client version must match that of the Kinetica
server.  When the versions do not match, the API will print a warning.  Often,
there are breaking changes between versions, so it is critical that they match.
For example, Kinetica 6.2 and 7.0 have incompatible changes, so the 6.2.x.y
versions of the Python API would NOT be compatible with 7.0.a.b versions, but
note that Kinetica 7.0 and 7.1 will remain compatible.

To install this package, run 'python setup.py install' in the root directory of
the repo.  Note that due to the in-house compiled C-module dependency, this
package must be installed, and simply copying gpudb.py or having a link to it
will not work.

There is also an example file in the example directory.

The documentation can be found at http://www.kinetica.com/docs/7.1/index.html.  
The python specific documentation can be found at:

*   http://www.kinetica.com/docs/7.1/tutorials/python_guide.html
*   http://www.kinetica.com/docs/7.1/api/python/index.html


For changes to the client-side API, please refer to CHANGELOG.md.  For
changes to GPUdb functions, please refer to CHANGELOG-FUNCTIONS.md.


Troubleshooting

* If you get an error when running pip like

```
  "Traceback ... File "/bin/pip", line 5, in <module> from pkg_resources import load_entry_point"
```

please try upgrading pip with command:

```
    python -m pip install --upgrade --force pip
```
 
* If you get an error when running pip like
```
    "Exception: Traceback ... File "/usr/lib/python2.7/site-packages/pip/basecommand.py", line 215, in main status = self.run(options, args)"
```

please try downgrading your version of pip setuptools with command:

```
    pip install setuptools==33.1.1
```

### GPUdb Table Monitor Client API

This is a new API introduced in v7.0.17.0 to facilitate working with
the table monitors created on the server to watch for insert, update
and delete operations on a table.

The main class to use is `GPUdbTableMonitorBase` and the idea is to create
an instance of that class and pass the relevant callbacks for insert,
update and delete notifications to be received as they are received
from the ZMQ socket exposed by the table monitor.

In the current implementation, the `GPUdbTableMonitorBase` class is designed to
handle a single Kinetica table.

It is possible to customize the behaviour of the `GPUdbTableMonitorBase`
class using an instance of `GPUdbTableMonitorBase.Options` class.

The options keys `operation_list` and `table_monitor_topic_id_list` are
mutually exclusive. Both cannot be given in the same `Options` instance.

```python
options = GPUdbTableMonitorBase.Options(
                                    _dict=dict(
                                    operation_list = None,
                                    notification_list = [NotificationEventType.TABLE_ALTERED,
                                    NotificationEventType.TABLE_DROPPED],
                                    terminate_on_table_altered=True,
                                    terminate_on_connection_lost=True,
                                    check_gpudb_and_table_state_counter=500,
                                    decode_failure_threshold=5,
                                    ha_check_threshold=10,
                                    zmq_polling_interval=1000,
                                    table_monitor_topic_id_list=["oEdnBcnFw5xArIPbpxm9tA==","0qpcEpoMR+x7tBNDZ4lMhg==","PBvWoh0Dcmz8nr3ce8zW3w=="]
                                ))

options = GPUdbTableMonitorBase.Options(
                                    _dict=dict(
                                    operation_list = [TableEventType.INSERT],
                                    notification_list = [NotificationEventType.TABLE_ALTERED,
                                    NotificationEventType.TABLE_DROPPED],
                                    terminate_on_table_altered=True,
                                    terminate_on_connection_lost=True,
                                    check_gpudb_and_table_state_counter=500,
                                    decode_failure_threshold=5,
                                    ha_check_threshold=10,
                                    zmq_polling_interval=1000,
                                    table_monitor_topic_id_list=None
                                ))


```

### `GPUdbTableMonitorBase.Options` Property Details

| No\. | Property Name                            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | Default Value                         |
|------|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| 1    | **operation_list**                          | This is a list which can be passed values of TableEventType enum like 'TableEventType.INSERT', 'TableEventType.UPDATE' etc. The list can contain a maximum of three elements one for each type in the enum\.                                                                                                                                                                                                                                                                                                                                       | *TableEventType.INSERT*                |
| 2    | **notification_list**                     | This is list which can be passed values of type<br>NotificationEventType enum like NotificationEventType.                                                                                                                                                                                                                                                                                                                                                                                                                                            | *NotificationEventType.TABLE_DROPPED* |
| 3    | **terminate_on_table\_altered**            | This is a boolean value indicating whether a table monitor is to be<br>terminated or not when a change in the table schema is detected.                                                                                                                                                                                                                                                                                                                                                                                                              | *True*                                  |
| 4    | **terminate_on_connection_lost**          | This is a boolean value indicating whether the table monitor is to be<br>terminated or not if the communication with GPUdb is broken for some<br>reason.                                                                                                                                                                                                                                                                                                                                                                                             | *True*                                  |
| 5    | **check_gpudb_and_table_state_counter** | This is a counter which<br>defines a threshold to check the state of GPUdb and the table to invoke<br>methods to activate HA if needed. It is mainly counted in the main loop<br>in idle state when polling the ZMQ socket has returned nothing\.                                                                                                                                                                                                                                                                                                    | *500*                                   |
| 6    | **decode_failure_threshold**               | This is a value (in seconds) to restrict<br>the number of times the program tries to decode a message after<br>having failed the first time, probably due to an alteration in the<br>table schema.                                                                                                                                                                                                                                                                                                                                                 | *5 seconds*                             |
| 7    | **ha_check_threshold**                     | This is a value (in seconds) to set the limit<br>to the number of seconds the program checks to activate HA when<br>needed.                                                                                                                                                                                                                                                                                                                                                                                                                        | *10 seconds*                            |
| 8    | **zmq_polling_interval**                   | This option controls the time interval to<br>set the timeout for ZMQ socket polling\. This is a value specified<br>in milliseconds.                                                                                                                                                                                                                                                                                                                                                                                                                  | *1000 milliseconds*                     |
| 9    | **table_monitor_topic_id_list**          | In case the table monitors for the              given table are already existing, the users can pass in the              topic_ids so that the messages can be subscribed to without the need             to create the monitors. The maximum number of elements in this list<br>can be three one topic_id for each table monitor for insert, update<br>and delete. Passing None for this mandates passing a valid value<br>for 'operation_list'. Passing both as None is not allowed as also<br>passing valid values for both is not allowed. | *[TableEventType.INSERT]*            |


The callbacks are passed in using an instance of the class `GPUdbTableMonitorBase.Callbacks`.

#### The details on the callbacks that could be used are as follows

| Name                | Purpose                                                                                                                                                                                                                                           | Payload Type                                                                  |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| **cb_insert_raw**     | There could be cases where the payload from ZMQ could be needed as it is since the<br> decoding and further downstream processing need to be done<br> at a later stage. The purpose of this callback is to hand<br>this data over to the users. | The type of the payload is the raw binary data in a list                      |
| **cb_insert_decoded** | There could be cases where the payload from ZMQ could be needed properly decoded since the<br> further downstream processing need to be done<br> at a later stage. The purpose of this callback is to hand<br>this data over to the users.  | The type of the payload is the decoded data in a list                         |
| **cb_updated**         | This callback carries the information about the number of records actually updated                                                                                                                                                                | The type of the payload is an integer indicating the count of records updated |
| **cb_deleted**         | This callback carries the information about the number of records actually deleted                                                                                                                                                                | The type of the payload is an integer indicating the count of records updated |
| **cb_table_dropped**  | This callback carries the information about the name of the table dropped                                                                                                                                                                         | The type of the payload is 'str' indicating the name of the table             |




As an example of creating the Callbacks class please have a look at a
code snippet for the class as given below:

```python
class GPUdbTableMonitorExampleImpl(GPUdbTableMonitorBase):
    """ An implementation which just passes on the received objects
        to a simple Queue which is passed in as an argument to the constructor
        of this class.

        This is an example implementation which can be used for simple cases
        to receive various notifications as they are received from the server
        as a result of operations on a given table.

    """

    def __init__(self, db, tablename,
                 record_queue, options = None):
        """ Constructor for GPUdbTableMonitorImpl class

        Args:
            db (GPUdb):
                The handle to the GPUdb
            
            tablename (str):
                Name of the table to create the monitor for
            
            record_queue (queue.Queue):
                A Queue instance where notifications along with payloads can be
                passed into for client to consume and act upon
            
            options (GPUdbTableMonitorBase.Options):
                Options instance which is passed on to the super class
                GPUdbTableMonitorBase constructor
        """
        callbacks = GPUdbTableMonitorBase.Callbacks(cb_insert_raw=self.on_insert_raw,
                                                cb_insert_decoded=self.on_insert_decoded,
                                                cb_updated=self.on_update,
                                                cb_deleted=self.on_delete,
                                                cb_table_dropped=self.on_table_dropped
                                                )
        super(GPUdbTableMonitorExampleImpl, self).__init__(
            db, tablename,
            callbacks, options=options)

        self.record_queue = record_queue
    def on_insert_raw(self, payload):
        """

        Args:
            payload:
        """
        self.logger.info("Payload received : %s " % payload)
        table_event = TableEvent(TableEventType.INSERT,
                                 count=-1, record_list=list(payload))
        self.record_queue.put(table_event)

    def on_insert_decoded(self, payload):
        """

        Args:
            payload:
        """
        self.logger.info("Payload received : %s " % payload)
        table_event = TableEvent(TableEventType.INSERT,
                                 count=-1, record_list=payload)
        self.record_queue.put(table_event)

    def on_update(self, count):
        """

        Args:
            count:
        """
        self.logger.info("Update count : %s " % count)
        table_event = TableEvent(TableEventType.UPDATE, count=count)
        self.record_queue.put(table_event)

    def on_delete(self, count):
        """

        Args:
            count:
        """
        self.logger.info("Delete count : %s " % count)
        table_event = TableEvent(TableEventType.DELETE, count=count)
        self.record_queue.put(table_event)

    def on_table_dropped(self, table_name):
        """
        Args:
            table_name:

        """
        self.logger.error("Table %s dropped " % self.table_name)
        notif_event = NotificationEvent(NotificationEventType.TABLE_DROPPED,
                                        table_name)
        self.record_queue.put(notif_event)

```

### Examples

1. This example uses the class `GPUdbTableMonitor` to demonstrate the how to use 
   the default implementation provided by Kinetica to enable a fast bootstrapping
   for the first time API users. It provides the callbacks already configured 
   and the class can be inherited from to override the callbacks so that some
   other processing needed by a specific use case can easily be done. The example
   shown here does not override the class `GPUdbTableMonitor` and uses it as is.
   So, the default behavior that will be observed is just a log of the event 
   payloads as they are received by the callbacks. 
   The link to this example is - 
   [table_monitor_example_default_impl.py - using GPUdbTableMonitor](./examples/table_monitor_example_default_impl.py)
2. This example demonstrates the use of `GPUdbTableMonitorBase` class from an
   example client class (`TableMonitorExampleClient`) where the instance of
   `GPUdbTableMonitorBase` is used by composition.
   The `TableMonitorExampleClient` class contains an instance of the
   `GPUdbTableMonitorBase` class. The link to this examples is -
   [table_monitor_example_basic_first.py - Basic example](./examples/table_monitor_example_basic_first.py)
3. This example is another variant of the previous example where the 
   `TableMonitorExampleClient` class is created as it was earlier but the 
   `GPUdbTableMonitorBase` class is instantiated separately using the callbacks
   defined by the `TableMonitorExampleClient` class. Both the class instances are
   constructed separately and then the method `start_monitor` is called on the 
   `GPUdbTableMonitor` class to start the monitor. The link to this example 
   is - [table_monitor_example_basic_second.py - Basic example](./examples/table_monitor_example_basic_second.py)
4. This example uses a class `QueuedGPUdbTableMonitor` which inherits from the
   class `GPUdbTableMonitorBase` and implements the callbacks. Additionally this
   class has a Queue instance which is shared with a client class 
   `TableMonitorExampleClient` which inherits from Thread and runs in its own
   thread. As the client receives notifications it just pushes them into the 
   shared Queue and the class `TableMonitorExampleClient` consumes them from the
   shared Queue and displays them in the console. The link to this example is -
   [table_monitor_example_queued_impl.py - using QueuedGPUdbTableMonitor](./examples/table_monitor_example_queued_impl.py)