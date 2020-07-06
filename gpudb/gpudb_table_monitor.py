import copy
import threading
import types
import time
import uuid

import zmq

try:
    from io import BytesIO
except:
    from cStringIO import StringIO as BytesIO
try:
    import httplib
except:
    import http.client as httplib

import os, sys
import json
import logging

# We'll need to do python 2 vs. 3 things in many places
IS_PYTHON_3 = (sys.version_info[0] >= 3)  # checking the major component
IS_PYTHON_27_OR_ABOVE = sys.version_info >= (2, 7)

if IS_PYTHON_3:
    long = int
    basestring = str

    class unicode:
        pass

# The absolute path of this gpudb.py module for importing local packages
gpudb_module_path = __file__
if gpudb_module_path[
   len(gpudb_module_path) - 3:] == "pyc":  # allow symlinks to gpudb.py
    gpudb_module_path = gpudb_module_path[0:len(gpudb_module_path) - 1]
if os.path.islink(gpudb_module_path):  # allow symlinks to gpudb.py
    gpudb_module_path = os.readlink(gpudb_module_path)
if not os.path.isabs(gpudb_module_path):  # take care of relative symlinks
    gpudb_module_path = os.path.join(os.path.dirname(__file__),
                                     gpudb_module_path)
gpudb_module_path = os.path.dirname(os.path.abspath(gpudb_module_path))

# Search for our modules first, probably don't need imp or virt envs.
if not gpudb_module_path in sys.path:
    sys.path.insert(1, gpudb_module_path)
if not gpudb_module_path + "/packages" in sys.path:
    sys.path.insert(1, gpudb_module_path + "/packages")

from protocol import RecordType

if IS_PYTHON_3:
    from gpudb.gpudb import GPUdb, GPUdbRecord, GPUdbException, \
        GPUdbConnectionException
else:
    from gpudb import GPUdb, GPUdbRecord, GPUdbException, \
    GPUdbConnectionException

import enum34 as enum

try:
    import queue
except ImportError:
    import Queue as queue

HA_CHECK_THRESHOLD_SECS = 10
DECODE_FAILURE_THRESHOLD_SECS = 5


# -----------------------------------------------------------------

class TableEventType(enum.Enum):
    """ Enum for table monitor event types

    """
    INSERT = 1
    UPDATE = 2
    DELETE = 3


class TableEvent(object):
    """ This class wraps up a table event type (TableEventType)
        and the corresponding data , a record list for insert type
        and count for delete and update

    """

    def __init__(self, event_type, count=None, record_list=None):
        if not isinstance(event_type, TableEventType):
            raise GPUdbException(
                "Event Type must be of type TableEventType ...")
        else:
            self.__table_event_type = event_type

        if count is not None:
            try:
                self.__count = int(count)
            except Exception as e:
                raise GPUdbException(e)

        if record_list is not None and not isinstance(record_list, list):
            raise GPUdbException("record_list must be of type list ...")
        else:
            self.__records = record_list

    @property
    def table_event_type(self):
        return self.__table_event_type

    @table_event_type.setter
    def table_event_type(self, event_type):
        self.__table_event_type = event_type

    @property
    def count(self):
        return self.__count

    @count.setter
    def count(self, count):
        try:
            self.__count = int(count)
        except Exception as e:
            raise GPUdbException(e)

    @property
    def records(self):
        return self.__records

    @records.setter
    def records(self, record_list):
        if record_list is not None and not isinstance(record_list, list):
            raise GPUdbException("record_list must be of type list ...")
        else:
            self.__records = record_list


class NotificationEventType(enum.Enum):
    TABLE_ALTERED = 1
    TABLE_DROPPED = 2
    CONNECTION_LOST = 3


class NotificationEvent(object):
    def __init__(self, event_type, message=None):
        if not isinstance(event_type, NotificationEventType):
            raise GPUdbException(
                "Event Type must be of type NotificationEventType ...")
        # else:
        #     self.__notification_event_type = event_type

        if message is not None and not isinstance(message,
                                                  (basestring, unicode)):
            raise GPUdbException("message must be of type string ...")
        else:
            self.__message = message

    @property
    def message(self):
        return self.__message

    @message.setter
    def message(self, message):
        if message is not None and not isinstance(message,
                                                  (basestring, unicode)):
            raise GPUdbException("message must be of type string ...")
        else:
            self.__message = message


class BaseTask(threading.Thread):
    """ This is the base Task class from which all other tasks are derived
        that run the specific monitors for insert, update and delete etc.

    """

    def __init__(self, 
                 db, 
                 table_name, 
                 topic_id_to_mode_map, 
                 table_event=TableEventType.INSERT, 
                 options=None, 
                 callbacks=None, 
                 id=None):

        """
        Constructor for BaseTask class, generally will not be needed to be 
        called directly, will be called by one of the subclasses 
        InsertWatcherTask, UpdateWatcherTask or DeleteWatcherTask

        Args:

        db (GPUdb) : Handle to GPUdb instance
        table_name (str): Name of the table to create the monitor for
        table_event (TableEventType): Enum of TableEventType
        options (GPUdbTableMonitor.Options): Options to configure GPUdbTableMonitor
        callbacks (GPUdbTableMonitor.Callbacks): Callbacks passed by user to be
            called on various events
        topic_id_to_mode_map (dict): map to store topic_id to mode string like
            'insert', 'update' or 'delete'

        Raises:
            GPUdbException: 
        """
                
        super(BaseTask, self).__init__()

        if db is None or not isinstance(db, GPUdb):
            raise GPUdbException("db must be of type GPUdb")
        self.db = db

        if table_name is None or not isinstance(table_name, (basestring, unicode)):
            raise GPUdbException("table_name must be a string")
        self.table_name = table_name

        self.event_type = table_event
        self.id = id

        if not isinstance(options, GPUdbTableMonitorBase.Options):
            options = GPUdbTableMonitorBase.Options.default()

        self._options = options

        if not isinstance(callbacks, GPUdbTableMonitorBase.Callbacks):
            raise GPUdbException("callbacks must be of type : "
                                 "GPUdbTableMonitor.Callbacks")
        else:
            self._callbacks = callbacks

        if not isinstance(topic_id_to_mode_map, dict):
            raise GPUdbException("topic_id_to_mode_map must be of type : "
                                 "dict")
        else:
            self._topic_id_to_mode_map = topic_id_to_mode_map

        # self.config = self._monitor.options
        self.type_id = ""
        self.type_schema = ""
        self.topic_id = ""
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.kill = False
        self.zmq_url = 'tcp://' + self.db.host + ':9002'
        self.check_gpudb_and_table_state_count = 0
        self.full_url = self.db.gpudb_full_url

        # Setup the logger for this instance
        self._id = str(uuid.uuid4())
        self.__logger = logging.getLogger("gpudb_table_monitor.BaseTask_instance_" + self._id)

        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)-8s {%("
                                       "funcName)s:%(lineno)d} %(message)s",
                                       "%Y-%m-%d %H:%M:%S")
        handler.setFormatter( formatter )

        self.__logger.addHandler( handler )

        # Prevent logging statements from being duplicated
        self.__logger.propagate = False

    # End __init__ BaseTask

    def setup(self):
        """This method sets up the internal state variables of the
        GPUdbTableMonitor object like type_id, type_schema and create the
        table monitor and connects to the ZMQ topic.

        """

        self.type_id, self.type_schema = self.__get_type_and_schema_for_table()
        if self.type_schema is not None and self.type_id is not None:
            self.record_type = RecordType.from_type_schema(
                label="",
                type_schema=self.type_schema,
                properties={})
        else:
            raise GPUdbException(
                "Failed to retrieve type_schema and type_id for table {}".format(
                    self.table_name))

        if self._options.table_monitor_topic_id_list is None:
            # The user hasn't passed any topic_id to connect to as part of
            # GPUdbTableMonitorOptions
            if self._create_table_monitor(table_event=self.event_type):
                self._connect_to_topic(self.zmq_url, self.topic_id)
        else:
            # The user has passed in one or more topic_ids as part of the
            # GPUdbTableMonitorOptions so we need to connect to them instead
            # of creating new table monitors. The lookup is being done from
            # the map 'self._monitor.topic_id_to_mode_map' which was setup
            # earlier.
            try:
                if self.event_type == TableEventType.INSERT:
                    topic_id = self._topic_id_to_mode_map['insert']
                elif self.event_type == TableEventType.UPDATE:
                    topic_id = self._topic_id_to_mode_map['update']
                else:
                    topic_id = self._topic_id_to_mode_map['delete']

                self.topic_id = topic_id
                self._connect_to_topic(self.zmq_url, topic_id)
            except KeyError as ke:
                raise GPUdbException(ke)

    # End setup BaseTask

    def _create_table_monitor(self, table_event):
        """This method creates the table monitor for the table name passed
        in as a parameter to the constructor of the table monitor class. It
        also caches the topic_id and the table schema in instance variables
        so that decoding of the messages received could be done.

        Returns: True or False

        """
        try:
            if table_event == TableEventType.DELETE:
                retval = self.db.create_table_monitor(self.table_name,
                                                      options={
                                                          'event': 'delete'})
            elif table_event == TableEventType.INSERT:
                retval = self.db.create_table_monitor(self.table_name,
                                                      options={
                                                          'event': 'insert'})
            else:
                retval = self.db.create_table_monitor(self.table_name,
                                                      options={
                                                          'event': 'update'})

            # Retain the topic ID, used in verifying the queued messages'
            # source and removing the table monitor at the end
            self.topic_id = retval["topic_id"]

            # Retain the type schema for decoding queued messages
            self.type_schema = retval["type_schema"]
            return True
        except GPUdbException as gpe:
            self.__logger.error(gpe.message)
            return False
    # End _create_table_monitor BaseTask

    def _remove_table_monitor(self):
        """ Remove the established table monitor, by topic ID
        """
        self.db.clear_table_monitor(self.topic_id)

    # End _remove_table_monitor BaseTask

    def _connect_to_topic(self, table_monitor_queue_url, topic_id):

        """ Create a connection to the message queue published to by the
        table monitor, filtering messages for the specific topic ID returned
        by the table monitor creation call

        """
        # Connect to queue using specified table monitor URL and topic ID
        self.__logger.debug("Starting...")
        self.socket.connect(table_monitor_queue_url)

        if sys.version_info[:3] > (3, 0):
            topicid = "".join(chr(x) for x in bytearray(topic_id, 'utf-8'))
        else:
            topicid = topic_id.decode('utf-8')

        self.socket.setsockopt_string(zmq.SUBSCRIBE, topicid)

        self.__logger.debug(" Started!")

    # End _connect_to_topic BaseTask

    def _disconnect_from_topic(self):
        """ This method closes the ZMQ socket and terminates the context
        """
        self.__logger.debug(" Stopping...")
        self.socket.close()
        self.context.term()
        self.__logger.debug(" Stopped!")

    # End _disconnect_from_topic BaseTask

    def run(self):
        """ Process queued messages until this client is stopped externally:

            * Poll the message queue every second for new messages. When the
            client is stopped externally, finish processing any messages in
            the current batch, and then disconnect from the message queue and
            remove the table monitor
        """
        while not self.kill:
            try:
                self._fetch_message()
            except zmq.ZMQError as zmqe:
                self.__logger.error("ZMQ connection error : %s" % zmqe.message)
                # Try to re-create the table monitor, resorting to HA
                if not self.__recreate_table_monitor():
                    self.kill = True
        # End of while loop
        # Clean up when this table monitor is stopped
        self._disconnect_from_topic()
        self._remove_table_monitor()
        # Signal the clients that they need to terminate as well
        # self.record_queue.put(None)

    # End run BaseTask

    def stop(self):
        """ This is a private method which just terminates the background
        thread which subscribes to the ZMQ topic and receives messages from it.

        """
        self.kill = True

    # End stop BaseTask

    def _try_decoding_on_table_altered(self, message_data, message_list=None):
        """This method tries to decode with the new type schema in case a
            table has been altered. The method will retry forever and would
            only fail if the 'DECODE_FAILURE_THRESHOLD_SECS' seconds have
            elapsed and still the decoding of the message has failed.

        Args:
            message_data:
            The raw message data (binary)

            message_list:
            The list of messages used as an accumulator for decoded
            messages.

        Returns:
            decoded:
            boolean True or False depending on success or failure

            record:
            The decoded record if there is one.

        """
        decoded = False
        record = None
        start_time = round(time.time())

        while True:
            try:
                # retry with refreshed type details id
                # and schema
                new_type_id, new_type_schema = self.__get_type_and_schema_for_table()

                self.record_type = RecordType.from_type_schema(
                                        label="",
                                        type_schema=new_type_schema,
                                        properties={})

                record = dict( GPUdbRecord.decode_binary_data(self.record_type, message_data)[0] )

                if message_list is not None:
                    message_list.append(record)
                # Update the instance variables on
                # success
                self.type_id, self.type_schema = new_type_id, new_type_schema
                # Break from while loop, if decoding is
                # successful with updated schema
                decoded = True
                break
            except Exception as e:
                self.__logger.error("Exception received "
                                "while decoding : "
                                "%s" % str(e))
                self.__logger.error(
                    "Failed to decode message %s with "
                    "updated schema %s" % message_data,
                    self.type_schema)

            if (round(time.time()) - start_time) >= DECODE_FAILURE_THRESHOLD_SECS:
                break
        # end while True

        return decoded, record

    # End _try_decoding_on_table_altered BaseTask

    def _check_state_on_no_zmq_message(self):
        """This method checks for table existence and other sanity checks while
        the main message processing loop is idle because ZMQ on
        successful polling returned nothing.

        Returns: Nothing

        """
        self.__logger.debug("In __check_state_on_no_zmq_message ...")

        self.check_gpudb_and_table_state_count += 1

        if self.check_gpudb_and_table_state_count == self._options.check_gpudb_and_table_state_counter:
            # Reached the configured counter value, reset the
            # counter and process further
            self.check_gpudb_and_table_state_count = 0

            self.__logger.debug("In __check_state_on_no_zmq_message : COUNT THRESHOLD REACHED")

            try:
                # Check whether the table is still valid
                table_exists = self.db.has_table(self.table_name, options={})['table_exists']

                current_full_url = self.full_url

                if table_exists:
                    if current_full_url != self.db.gpudb_full_url:
                        # HA taken over

                        # Cache the full_url value
                        self.full_url = self.db.gpudb_full_url

                        self.__logger.debug("{} :: HA Switchover "
                                          "happened : Current_full_url = {} "
                                          "and "
                                          "new_gpudb_full_url = {}".format(self.id, current_full_url, self.db.gpudb_full_url))

                        new_type_id, new_type_schema = self.__get_type_and_schema_for_table()

                        self.__logger.debug("Old type_id = {} : New type_id = {}".format(self.type_id, new_type_id))

                        if self.type_id != new_type_id and self._options.terminate_on_table_altered:
                            # Means table has been altered
                            # Check first whether to continue
                            self._quit_on_exception(
                                event_type=None,
                                message="Table altered, "
                                        "terminating ...")

                        # create table monitors if not terminated due to a
                        # table alteration
                        self._create_table_monitor(
                            table_event=self.event_type)
                        self.type_id = new_type_id
                        self.type_schema = new_type_schema

                        # Connect to the new topic_id
                        self.zmq_url = "tcp://" + self.db.host + ":9002"
                        self._connect_to_topic(
                            table_monitor_queue_url=self.zmq_url,
                            topic_id=self.topic_id)
                else:
                    self._quit_on_exception(
                        NotificationEventType.TABLE_DROPPED,
                        "Table %s does not "
                        "exist anymore ..."
                        % self.table_name)

            except GPUdbException as gpe:
                if isinstance(gpe, GPUdbConnectionException):
                    self.__logger.error("GpuDb error : %s" % gpe.message)
            except Exception as e:
                self._quit_on_exception(event_type=None, message=str(e))

    # End _check_state_on_no_zmq_message BaseTask


    def _quit_on_exception(self, event_type, message, topic_id_recvd=None,
                           message_list=None):
        """ This method is invoked on an exception which could be difficult
        to recover from and then it will simply terminate the background
        thread and exit cleanly. It will also indicate the clients of the
        table monitor by placing a special object 'None' in the shared Queue
        so that the clients know that they should terminate as well and can
        exit gracefully.

        Args: message: The exact exception message that could be logged for
        further troubleshooting
        """
        if topic_id_recvd is not None and message_list is not None:
            if len(message_list) > 0 and event_type == TableEventType.INSERT:
                self._callbacks.cb_insert_decoded(copy.deepcopy(message_list))

            # Remove the messages as they may not be
            # valid anymore
            del message_list[:]

        self.__logger.error(message)

        if event_type == NotificationEventType.TABLE_DROPPED:
            self._callbacks.cb_table_dropped(message)

        # Connection to GPUDb failed or some other GPUDb failure, might as
        # well quit
        self.stop()

    # End _quit_on_exception BaseTask

    def __recreate_table_monitor(self):
        """ This method calls has_table method on the gpudb object with the
        purpose of activating HA if needed and caches the 'gpudb_full_url'
        value which is compared with the one already cached to determine
        whether the HA failover has been successful or not. In case, the HA
        failover has been successful it re-creates the table monitor on the
        HA afresh so that the table monitor can continue and survive server
        outages. It is configured using
        'GPUdbTableMonitorConstants.HA_CHECK_THRESHOLD' which is used to
        retry a certain number of times before giving up on HA switchover.

        Returns: If the HA switchover is successful it returns True otherwise
        if the counter expires and still the HA switchover fails it returns
        False.

        """
        table_monitor_created = False
        start_time = round(time.time())

        while True:
            try:
                # The following call should trigger HA/failover
                table_exists = self.db.has_table(self.table_name, options={})[
                    'table_exists']

                current_full_url = self.full_url
                if table_exists:

                    if current_full_url != self.db.gpudb_full_url:
                        # HA taken over

                        # Cache the full_url value
                        self.full_url = self.db.gpudb_full_url

                        self.__logger.debug("{} :: HA Switchover "
                                        "happened : Current_full_url = {} "
                                        "and "
                                        "new_gpudb_full_url = {}".format(
                                            self.id, current_full_url,
                                            self.db.gpudb_full_url))

                        self._create_table_monitor(table_event=self.event_type)

                        # Connect to the new topic_id
                        self.zmq_url = "tcp://" + self.db.host + ":9002"
                        self._connect_to_topic(
                                            table_monitor_queue_url=self.zmq_url,
                                            topic_id=self.topic_id)

                        table_monitor_created = True
                        break
                else:
                    # Table does not exist
                    if self._callbacks is not None and self._callbacks.cb_table_dropped is not None:
                        self._callbacks.cb_table_dropped(self.table_name)
                    break

            except GPUdbException as gpe:
                self.__logger.error(gpe.message)

            if (round(time.time()) - start_time) >= HA_CHECK_THRESHOLD_SECS:
                break
        # end while True

        return table_monitor_created

    # End __recreate_table_monitor BaseTask

    def __get_type_and_schema_for_table(self):
        """ This method retrieves the table schema and type_id and returns a
        tuple composed of the values Args: table_name: The name of the table
        for which the type_id and schema are to be retrieved

        Returns: A tuple containing the type_id and schema

        """
        try:
            show_table_response = self.db.show_table(
                table_name=self.table_name, options={})
            # Retrieve the latest schema
            latest_type_schema = show_table_response['type_schemas'][0]
            # Retrieve the latest type_id
            latest_type_id = show_table_response['type_ids'][0]
            return latest_type_id, latest_type_schema
        except GPUdbException as gpe:
            self.__logger.error(gpe.message)
            return None, None

    # End __get_type_and_schema_for_table BaseTask

    def execute(self):
        """ This method does the job of executing the task. It calls in sequence
            _connect, start and _disconnect.
            _connect connects to the ZMQ socket and sets up everything
            start starts the background thread
            _disconnect drops the ZMQ socket connection.

            This is actually a template method where _connect and _disconnect are
            implemented by the derived classes.

        """
        self._connect()
        self.start()
        self._disconnect()

    # End execute BaseTask

    def _connect(self):
        """ Implemented by the derived classes InsertWatcherTask,
            UpdateWatcherTask and DeleteWatcherTask.

        """
        raise NotImplementedError(
            "Method '_connect' of 'BaseTask' must be overridden in the derived classes")

    # End _connect BaseTask

    def _fetch_message(self):
        """ This method is called by the run method which polls the socket and calls
            the method _process_message for doing the actual processing.
            _process_message is once again overridden in the derived classes.

        """
        ret = self.socket.poll(self._options.zmq_polling_interval)

        if ret != 0:
            self.__logger.debug("Received message .. ")
            messages = self.socket.recv_multipart()
            self._process_message(messages)

        else:
            # ret==0, meaning nothing received from socket.
            # Process all the other cases here since there is no
            # message to be processed.
            self._check_state_on_no_zmq_message()

    # End _fetch_message BaseTask

    def _process_message(self, messages):
        """ This method does the actual processing of the messages received
            from the socket and suitably calls the event handlers and
            callback methods.

            The implementations differ and are taken care of in the derived
            classes since insert and delete/update are handled completely
            differently.

            This method has to be overridden in the derived classes

        Args:
            messages:
        """
        raise NotImplementedError(
            "Method '_process_message' of 'BaseTask' must be overridden in the derived classes")

    # End _process_message BaseTask

    def _disconnect(self):
        """Implemented by the derived classes InsertWatcherTask,
            UpdateWatcherTask and DeleteWatcherTask.

        """
        raise NotImplementedError(
            "Method '_disconnect' of 'BaseTask' must be overridden in the derived classes")

    # End _disconnect BaseTask

    @property
    def logging_level(self):
        return self.__logger.level

    @logging_level.setter
    def logging_level(self, value):
        """
        This property sets the log level for this class and its derivatives.

        Args:
            value (logging.level): Default setting is logging.INFO

        Raises:
            GPUdbException: If the value passed is not one of logging.INFO
            or logging.DEBUG etc.
        """
        try:
            self.__logger.setLevel(value)
        except (ValueError, TypeError, Exception) as ex:
            raise GPUdbException("Invalid log level: '{}'".format(str(ex)))


# End class BaseTask


class InsertWatcherTask(BaseTask):
    """ This is the class which handles only inserts and subsequent processing
        of the messages received as a result of notifications from ZMQ on
        insertions of new records into the table.

    """

    def __init__(self, db, table_name, topic_id_to_mode_map,
                 table_event=TableEventType.INSERT,
                 options=None, callbacks=None):
        """
        [summary]

        Args:
        db (GPUdb) : Handle to GPUdb instance
        table_name (str): Name of the table to create the monitor for
        table_event (TableEventType): Enum of TableEventType
        options (GPUdbTableMonitor.Options): Options to configure GPUdbTableMonitor
        callbacks (GPUdbTableMonitor.Callbacks): Callbacks passed by user to be
            called on various events
        topic_id_to_mode_map (dict): map to store topic_id to mode string like
            'insert', 'update' or 'delete'

        """

        super(InsertWatcherTask, self).__init__(db,
                                                table_name,
                                                topic_id_to_mode_map,
                                                table_event=table_event,
                                                options=options,
                                                callbacks=callbacks,
                                                id='INSERT_' + table_name)
        self._callbacks = None if callbacks is None else callbacks
        self.__cb_insert_raw = None if self._callbacks is None else self._callbacks.cb_insert_raw
        self.__cb_insert_decoded = None if self._callbacks is None else self._callbacks.cb_insert_decoded

    def _connect(self):
        """

        """
        self.setup()

    def _process_message(self, messages):
        """ Process only messages assuming that they are inserts.

        Args:
            messages (list): Multi-part messages received from a single socket
            poll.
        """
        message_list = []

        if sys.version_info[0] == 2:
            topic_id_recvd = "".join(
                chr(x) for x in bytearray(messages[0], 'utf-8'))
        else:
            topic_id_recvd = str(messages[0], 'utf-8')

        self.__logger.info("Topic_id_received = " + topic_id_recvd)

        message_parsing_failed = False
        # Process all messages, skipping the (first) topic frame
        for message_index, message_data in enumerate(messages[1:]):

            # Decode the record from the message using the type
            # schema, initially returned during table monitor
            # creation
            try:
                record = dict(GPUdbRecord.decode_binary_data(self.record_type,
                                                             message_data)[0])

                # self.logger.debug("Topic Id = {} , record = {} "
                #                   .format(topic_id_recvd,
                #                           record))

                message_list.append(record)
            except Exception as e:
                # The exception could only be because of some
                # issue with decoding the data; possibly due to
                # a different schema resulting from a table
                # alteration.
                self.__logger.error("Exception received while decoding {}".format(str(e)))

                self.__logger.error("Failed to decode message {} "
                                  "with schema {}".format(message_data,
                                                          self.type_schema))

                message_parsing_failed = True

                # Introduce a configuration variable to
                # switch the behaviour of the table monitor
                # based on whether the table had undergone
                # any changes after the monitor was set up.
                # Check the configuration about the behaviour
                # on whether to continue with updated schema
                # or terminate the table monitor since the
                # table itself was altered.

                if self._options.terminate_on_table_altered:
                    # Call the callback anyway so that they can be
                    # consumed by the clients of the table monitor
                    if len(
                            message_list) > 0 and self.__cb_insert_decoded is not None:
                        try:
                            self.__cb_insert_decoded(message_list)
                        except Exception as e:
                            raise GPUdbException(e)

                    self._quit_on_exception(TableEventType.INSERT,
                                            "Table altered, "
                                            "terminating ...",
                                            topic_id_recvd,
                                            message_list)
                else:
                    # Try till the 'decode_failure_threshold' number
                    # of Secs is hit and then give up
                    decoded, record = self._try_decoding_on_table_altered(
                        message_data, message_list)
                    if not decoded:
                        message_parsing_failed = True
                        break
            try:
                if self.__cb_insert_raw is not None:
                    self.__cb_insert_raw(message_data)
            except Exception as e:
                self.__logger.error(e)
                raise GPUdbException(e)

        if not message_parsing_failed:
            self.__logger.debug("Received <%s> messages and one topic frame" % (len(messages) - 1))
            if self.__cb_insert_decoded is not None:
                try:
                    self.__cb_insert_decoded(message_list)
                except Exception as e:
                    raise GPUdbException(e)
        else:
            # Message parsing failed even after retrying the
            # preconfigured count times, give up and quit.
            if len(message_list) > 0 and self.__cb_insert_decoded is not None:
                try:
                    self.__cb_insert_decoded(message_list)
                except Exception as e:
                    raise GPUdbException(e)

            self._quit_on_exception(TableEventType.INSERT, "Table altered, terminating ...",
                                    topic_id_recvd, message_list)

    # End _process_message InsertWatcherTask(BaseTask)

    def _disconnect(self):
        """

        """
        self.stop()


class UpdateWatcherTask(BaseTask):
    """ This is the class which handles only updates and subsequent processing
        of the messages received as a result of notifications from ZMQ on
        updates to the records of a table.

    """

    def __init__(self, db, table_name, topic_id_to_mode_map,
                 table_event=TableEventType.UPDATE,
                 options=None,
                 callbacks=None,
                 ):

        """
        Constructor the the class UpdateWatcherTask which inherits from
        BaseTask

        Args:
        db (GPUdb) : Handle to GPUdb instance
        table_name (str): Name of the table to create the monitor for
        table_event (TableEventType): Enum of TableEventType
        options (GPUdbTableMonitor.Options): Options to configure GPUdbTableMonitor
        callbacks (GPUdbTableMonitor.Callbacks): Callbacks passed by user to be
            called on various events
        topic_id_to_mode_map (dict): map to store topic_id to mode string like
            'insert', 'update' or 'delete'
        """

        super(UpdateWatcherTask, self).__init__(db,
                                                table_name,
                                                topic_id_to_mode_map,
                                                table_event,
                                                options,
                                                callbacks,
                                                id='UPDATE_' + table_name)

        if callbacks is not None \
                and isinstance(callbacks, GPUdbTableMonitorBase.Callbacks):
            self._callbacks = callbacks
            self.__cb_update = self._callbacks.cb_updated
        else:
            self.__cb_update = None

    def _connect(self):
        """

        """
        self.setup()

    def _process_message(self, messages):
        """

        """

        if sys.version_info[0] == 2:
            topic_id_recvd = "".join(
                chr(x) for x in bytearray(messages[0], 'utf-8'))
        else:
            topic_id_recvd = str(messages[0], 'utf-8')

        message_parsing_failed = False

        # Process all messages, skipping the (first) topic frame

        # Decode the record from the message using the type
        # schema, initially returned during table monitor
        # creation
        retobj = None
        try:
            retobj = dict(
                GPUdbRecord.decode_binary_data(self.record_type, messages[1])[
                    0])

            self.__logger.debug("Topic Id = {} , record = {} "
                                .format(topic_id_recvd,
                                      retobj["count"]))
        except Exception as e:
            # The exception could only be because of some
            # issue with decoding the data; possibly due to
            # a different schema resulting from a table
            # alteration.
            self.__logger.error(
                "Exception received while decoding {}".format(
                    str(e)))
            self.__logger.error("Failed to decode message {} "
                              "with schema {}".format(
                                                        messages[1],
                                                        self.type_schema
                                                        ))
            message_parsing_failed = True

            # Introduce a configuration variable to
            # switch the behaviour of the table monitor
            # based on whether the table had undergone
            # any changes after the monitor was set up.
            # Check the configuration about the behaviour
            # on whether to continue with updated schema
            # or terminate the table monitor since the
            # table itself was altered.

            if self._options.terminate_on_table_altered:
                # Copy the existing messages to the
                # work_queue anyway so that they can be
                # consumed by the clients of the table monitor
                self._quit_on_exception(
                    event_type=None,
                    message="Table altered, "
                            "terminating ...",
                    topic_id_recvd=topic_id_recvd)
            else:
                # Try till the 'decode_failure_threshold' number
                # of Secs is hit and then give up
                decoded, record = self._try_decoding_on_table_altered(
                    messages[1])
                if not decoded:
                    message_parsing_failed = True

        try:
            if self.__cb_update is not None:
                self.__cb_update(retobj["count"])
        except Exception as e:
            self.__logger.error(e)
            raise GPUdbException(e)

        if message_parsing_failed:
            # Message parsing failed even after retrying the
            # preconfigured count times, give up and quit.
            self._quit_on_exception(event_type=None,
                                    message="Table altered, terminating ...",
                                    topic_id_recvd=topic_id_recvd)

    # End _process_message UpdateWatcherTask(BaseTask)

    def _disconnect(self):
        """

        """
        self.stop()


class DeleteWatcherTask(BaseTask):
    """ This is the class which handles only deletes and subsequent processing
        of the messages received as a result of notifications from ZMQ on
        on deletions of records of a table.

    """

    def __init__(self, db, table_name, topic_id_to_mode_map,
                 table_event=TableEventType.DELETE,
                 options=None, callbacks=None):
        """
        Constructor of the DeleteWatcherTask class

        Args:
        db (GPUdb) :
            Handle to GPUdb instance

        table_name (str):
            Name of the table to create the monitor for

        table_event (TableEventType):
            Enum value of TableEventType

        options (GPUdbTableMonitor.Options):
            Options to configure GPUdbTableMonitor

        callbacks (GPUdbTableMonitor.Callbacks):
            Callbacks passed by user to be called on various events

        topic_id_to_mode_map (dict):
            map to store topic_id to mode string like 'insert', 'update'
            or 'delete'

        """

        super(DeleteWatcherTask, self).__init__(db,
                                                table_name,
                                                topic_id_to_mode_map,
                                                table_event=table_event,
                                                options=options,
                                                callbacks=callbacks,
                                                id='DELETE_' + table_name)
        self._callbacks = None if callbacks is None else callbacks
        self.__cb_delete = None if self._callbacks is None else self._callbacks.cb_deleted

    def _connect(self):
        """

        """
        self.setup()

    def _process_message(self, messages):
        """

        """
        self.__logger.debug("Messages  = %s" % messages)

        if sys.version_info[0] == 2:
            topic_id_recvd = "".join(
                chr(x) for x in bytearray(messages[0], 'utf-8'))
        else:
            topic_id_recvd = str(messages[0], 'utf-8')

        message_parsing_failed = False

        # Process all messages, skipping the (first) topic frame

        # Decode the record from the message using the type
        # schema, initially returned during table monitor
        # creation
        retobj = None
        try:
            retobj = dict(
                GPUdbRecord.decode_binary_data(self.type_schema, messages[1])[
                    0])

            self.__logger.debug("Topic Id = {} , record = {} "
                                .format(topic_id_recvd,
                                      retobj["count"]))
        except Exception as e:
            # The exception could only be because of some
            # issue with decoding the data; possibly due to
            # a different schema resulting from a table
            # alteration.
            self.__logger.error(
                "Exception received while decoding {}".format(str(e)))

            self.__logger.error("Failed to decode message {} "
                              "with schema {}".format( messages[1], self.type_schema))

            message_parsing_failed = True

            # Introduce a configuration variable to
            # switch the behaviour of the table monitor
            # based on whether the table had undergone
            # any changes after the monitor was set up.
            # Check the configuration about the behaviour
            # on whether to continue with updated schema
            # or terminate the table monitor since the
            # table itself was altered.

            if self._options.terminate_on_table_altered:
                # Copy the existing messages to the
                # work_queue anyway so that they can be
                # consumed by the clients of the table monitor
                self._quit_on_exception(
                    event_type=None,
                    message="Table altered, "
                            "terminating ...",
                    topic_id_recvd=topic_id_recvd)
            else:
                # Try till the 'decode_failure_threshold' number
                # of Secs is hit and then give up
                decoded, record = self._try_decoding_on_table_altered(
                    messages[1])
                if not decoded:
                    message_parsing_failed = True

        try:
            if self.__cb_delete is not None:
                self.__cb_delete(retobj["count"])
        except Exception as e:
            self.__logger.error(e)
            raise GPUdbException(e)

        if message_parsing_failed:
            # Message parsing failed even after retrying the
            # preconfigured count times, give up and quit.
            self._quit_on_exception(event_type=None,
                                    message="Table altered, terminating ...",
                                    topic_id_recvd=topic_id_recvd)

    # End _process_message DeleteWatcherTask(BaseTask)

    def _disconnect(self):
        """

        """
        self.stop()


class GPUdbTableMonitorBase(object):
    """ This class is the main client side API class implementing most of the
        functionalities. It is extended by the class GPUdbTableMonitor which
        has the default event handlers implemented.

        The intended use is to override the callbacks in the GPUdbTableMonitor
        class to process further downstream processing.

        Several uses of this class has been shown in the file
        table_monitor_example_basic*.py in the directory ./examples.

        The readme contains the relevant links to navigate directly to the
        examples provided.

    """

    def __init__(self, db, tablename, callbacks, options=None):
        """

        Args:
            db (GPUdb):
            The GPUdb object which is created external to this
            class and passed in to facilitate calling different methods of the
            GPUdb API. This has to be pre-initialized and must have a valid
            value. If this is uninitialized then the constructor would raise a
            GPUdbException exception.

            tablename (str):
            The name of the Kinetica table for
            which a monitor is to be created. This must have a valid value and
            cannot be an empty string. In case this parameter does not have a
            valid value the constructor will raise a GPUdbException exception.

            callbacks (GPUdbTableMonitor.Callbacks): Instance of the Callbacks
                nested class of GPUdbTableMonitor class.

            options (GPUdbTableMonitorOptions):
            The class to encapsulate the various options that can be passed
            to a GPUdbTableMonitor object to alter the behaviour of the
            monitor instance. The details are given in the section of
            GPUdbTableMonitorOptions class.
        """
        super(GPUdbTableMonitorBase, self).__init__()

        global HA_CHECK_THRESHOLD_SECS
        global DECODE_FAILURE_THRESHOLD_SECS

        if not self.__check_params(db, tablename):
            raise GPUdbException("Both db and tablename need valid values ...")

        self.type_id = ""
        self.type_schema = ""
        self.db = db
        self.full_url = self.db.gpudb_full_url
        self.table_name = tablename
        self.check_gpudb_and_table_state_count = 0
        self.task_list = list()

        if not isinstance(callbacks, GPUdbTableMonitorBase.Callbacks):
            raise GPUdbException("callbacks must be of type : "
                                 "'GPUdbTableMonitor.Callbacks'")
        else:
            self.callbacks = callbacks

        self._topic_id_to_mode_map = dict()

        if options is None:
            # This is the default, created internally
            self.options = GPUdbTableMonitorBase.Options.default()
            self.__operation_list = self.options.operation_list
            HA_CHECK_THRESHOLD_SECS = self.options.ha_check_threshold
            DECODE_FAILURE_THRESHOLD_SECS = self.options.decode_failure_threshold
        else:
            # User has passed in options, check everything for validity
            if isinstance(options, GPUdbTableMonitorBase.Options):
                # Check whether passed in topic_ds if any are valid or not
                # If the topic_ids are not valid raise GPUdbException and
                # bail out

                try:
                    self.__check_options(options)

                    self.options = options
                    self.__operation_list = list()

                    if self.options.operation_list is None:
                        for mode in self._topic_id_to_mode_map.keys():
                            if mode == 'insert':
                                self.__operation_list.append(
                                    TableEventType.INSERT)
                            elif mode == 'update':
                                self.__operation_list.append(
                                    TableEventType.UPDATE)
                            else:
                                self.__operation_list.append(
                                    TableEventType.DELETE)
                    else:
                        self.__operation_list = self.options.operation_list

                    HA_CHECK_THRESHOLD_SECS = self.options.ha_check_threshold
                    DECODE_FAILURE_THRESHOLD_SECS = self.options.decode_failure_threshold
                except GPUdbException as ge:
                    self.__logger.error(ge.message)
                    raise GPUdbException(ge)

            else:
                raise GPUdbException("Passed in options is not of the expected "
                                     "type: Expected "
                                     "'GPUdbTableMonitor.Options' type")

        # Setup the logger for this instance
        self._id = str(uuid.uuid4())
        self.__logger = logging.getLogger("gpudb_table_monitor.GPUdbTableMonitorBase_instance_" + self._id)

        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)-8s {%("
                                       "funcName)s:%(lineno)d} %(message)s",
                                       "%Y-%m-%d %H:%M:%S")
        handler.setFormatter( formatter )

        self.__logger.addHandler( handler )

        # Prevent logging statements from being duplicated
        self.__logger.propagate = False

    # End __init__ GPUdbTableMonitorBase

    def __check_options(self, options):
        """

        Args:
            options:
        """
        if options is not None and options.table_monitor_topic_id_list is not None:

            if (options.operation_list is not None) and \
                ( not isinstance(options.operation_list, list) or
                    len(options.operation_list) == 0):
                raise GPUdbException(
                    "operation_list is not a valid list")

            if options.operation_list is not None:
                raise GPUdbException("Both 'operation_list' and "
                                     "'topic_id_list' cannot be "
                                     "specified; conflicting ...")

            # Check for correctness of argument - topic_id_list
            retval, self._topic_id_to_mode_map, self.type_id, self.type_schema = \
                self.__get_existing_monitors(
                    options.table_monitor_topic_id_list)

            if not retval:
                raise GPUdbException("'topic_id_list' passed in "
                                     "options is not valid ...")

    def __check_params(self, db, table_name):
        """ This method checks the parameters passed into the GPUdbTableMonitor
        constructor for correctness.

        Checks for existence of the table as well.

        Args:
            db: the GPUdb object needed to access the different APIs
            table_name: Name of the table to create the monitor for.

        Returns: Returns True or False if both the values are correct or wrong
        respectively.

        """
        gpu_db_value_correct = False
        table_name_value_correct = False

        if db is not None and isinstance(db, GPUdb):
            gpu_db_value_correct = True

        if table_name is not None and \
                (isinstance(table_name, (basestring, unicode)) and len(
                    table_name.strip()) > 0) and \
                gpu_db_value_correct and \
                db.has_table(table_name, options={})["table_exists"]:
            table_name_value_correct = True

        return gpu_db_value_correct and table_name_value_correct

    def __get_existing_monitors(self, topic_id_list):
        """This method checks whether the list of topic_ids passed in the config
            is valid by retrieving the topic_ids of the existent of table
            monitors for the given table.

        Args:
            topic_id_list:
            The list of topic_ids representing existing table monitors
        """
        show_table_resp = self.db.show_table(self.table_name)

        if not show_table_resp.is_ok():
            raise GPUdbException(show_table_resp.get_error_msg())

        type_id = show_table_resp["type_ids"][0]
        type_schema = show_table_resp["type_schemas"][0]

        table_info = show_table_resp['additional_info'][0]

        table_monitor_info = None
        retval = False

        if 'table_monitor' in table_info:
            table_monitor_info = json.loads(table_info['table_monitor'])
            table_monitor_topic_ids = table_monitor_info.values()
            retval = all(topic_id in topic_id_list for topic_id in
                         table_monitor_topic_ids)

        return retval, table_monitor_info, type_id, type_schema

    def start_monitor(self):
        """ Method to start the Table Monitor
            This the API called by the client to start the table monitor
        """
        for event_type in self.__operation_list:

            if event_type == TableEventType.INSERT:
                insert_task = InsertWatcherTask(self.db,
                                                self.table_name,
                                                self.topic_id_to_mode_map,
                                                table_event=event_type,
                                                options=self.options,
                                                callbacks=self.callbacks
                                                )
                insert_task.setup()
                insert_task.start()
                self.task_list.append(insert_task)
            elif event_type == TableEventType.UPDATE:
                update_task = UpdateWatcherTask(self.db,
                                                self.table_name,
                                                self.topic_id_to_mode_map,
                                                table_event=event_type,
                                                options=self.options,
                                                callbacks=self.callbacks,
                                                )
                update_task.setup()
                update_task.start()
                self.task_list.append(update_task)
            else:
                delete_task = DeleteWatcherTask(self.db,
                                                self.table_name,
                                                self.topic_id_to_mode_map,
                                                table_event=event_type,
                                                options=self.options,
                                                callbacks=self.callbacks,
                                                )
                delete_task.setup()
                delete_task.start()
                self.task_list.append(delete_task)

    def stop_monitor(self):
        """ Method to Stop the table monitor.
            This API is called by the client to stop the table monitor
        :rtype: bool

        """
        for task in self.task_list:
            task.stop()
            task.join()

    @property
    def topic_id_to_mode_map(self):
        """

        Returns:

        """
        return self._topic_id_to_mode_map


    @property
    def logging_level(self):
        return self.__logger.level

    @logging_level.setter
    def logging_level(self, value):
        """
        This property sets the log level for this class and its derivatives.

        Args:
            value (logging.level): Default setting is logging.INFO

        Raises:
            GPUdbException: If the value passed is not one of logging.INFO
            or logging.DEBUG etc.
        """
        try:
            self.__logger.setLevel(value)
        except (ValueError, TypeError, Exception) as ex:
            raise GPUdbException("Invalid log level: '{}'".format( str(ex) ))


    class Callbacks(object):
        """ This is an inner class to GPUdbTableMonitor to encapsulate all
        the callback methods needed to be passed in.

        """

        def __init__(
                self,
                cb_insert_raw=None,
                cb_insert_decoded=None,
                cb_updated=None,
                cb_deleted=None,
                cb_table_dropped=None):
            """

            Args:
                cb_insert_raw (object - method reference):
                    To be called when an insert notification is received and
                    the payload is the raw message before decoding
                cb_insert_decoded (object - method reference):
                    To be called when an insert notification is received and
                    the payload is the decoded message
                cb_updated (object - method reference):
                    To be called when an update notification is received and
                    the payload is the updated record count
                cb_deleted (object - method reference):
                    To be called when a delete notification is received and
                    the payload is the deleted record count
                cb_table_dropped (object - method reference):
                    To be called when a table deleted notification is received
                    and the payload a message saying which table is deleted

            Raises:
                GPUdbException
            """
            cb_list = [cb_insert_decoded, cb_insert_raw, cb_updated, cb_deleted,
                       cb_table_dropped]

            if self.__check_callback_types(cb_list):
                self.__cb_deleted = cb_deleted
                self.__cb_updated = cb_updated
                self.__cb_insert_decoded = cb_insert_decoded
                self.__cb_insert_raw = cb_insert_raw
                self.__cb_table_dropped = cb_table_dropped
            else:
                raise GPUdbException(
                    "One or more callback functions are not of valid function type "
                    "...")

        @property
        def cb_insert_raw(self):
            return self.__cb_insert_raw

        @property
        def cb_insert_decoded(self):
            return self.__cb_insert_decoded

        @property
        def cb_deleted(self):
            return self.__cb_deleted

        @property
        def cb_updated(self):
            return self.__cb_updated

        @property
        def cb_table_dropped(self):
            return self.__cb_table_dropped

        def __check_callback_types(self, callback_list):
            """ Tests whether each object in the list is a function object

            Args:
                callback_list:

            Returns:
                True/False (boolean): If all the elements in the list are of type
                callable it returns True else False

            """
            return all([self.__check_whether_function(func) for func in
                        callback_list])

        def __check_whether_function(self, func):
            """ Tests whether the object passed in is actually a function or not

            Args:
                func:

            Returns: True or False

            """
            return func is None or isinstance(func, (types.FunctionType,
                                                     types.BuiltinFunctionType,
                                                     types.MethodType,
                                                     types.BuiltinMethodType
                                                     )) \
                                                    or callable(func)
    # End of Callbacks nested class

    class Options(object):
        """
        Encapsulates the various options used to create a table monitor. The
        class is initialized with sensible defaults which can be overridden by
        the users of the class. The following options are supported :

        1. operation_list - This is a list which can be passed values of
            TableEventType enum like 'TableEventType.INSERT',
            'TableEventType.UPDATE' etc. The list can contain a maximum of three
            elements one for each type in the enum.

        2. notification_list - This is list which can be passed values of type
            NotificationEventType enum like NotificationEventType.

        3. terminate_on_table_altered -
            This is a boolean value indicating whether a table monitor is to be
            terminated or not when a change in the table schema is detected.

        4. terminate_on_connection_lost -
            This is a boolean value indicating whether the table monitor is to be
            terminated or not if the communication with GPUdb is broken for some
            reason.

        5. check_gpudb_and_table_state_counter -
            This is a counter which defines a threshold to check the state of
            GPUdb and the table to invoke methods to activate HA if needed. It
            is mainly counted in the main loop in idle state when polling the
            ZMQ socket has returned nothing.

        6. decode_failure_threshold -
            This is a value (in seconds) to restrict
            the number of times the program tries to decode a message after
            having failed the first time, probably due to an alteration in the
            table schema.

        7. ha_check_threshold -
            This is a value (in seconds) to set the limit
            to the number of seconds the program checks to activate HA when
            needed.

        8. zmq_polling_interval - This option controls the time interval to
            set the timeout for ZMQ socket polling. This is a value specified
            in milliseconds.

        9. table_monitor_topic_id_list - In case the table monitors for the
            given table are already existing, the users can pass in the
            topic_ids so that the messages can be subscribed to without the need
            to create the monitors. The maximum number of elements in this list
            can be three one topic_id for each table monitor for insert, update
            and delete. Passing None for this mandates passing a valid value
            for 'operation_list'. Passing both as None is not allowed as also
            passing valid values for both is not allowed.

        Example usage:
            options = GPUdbTableMonitor.Options(_dict=dict(
                                                        operation_list = None,
                                                        notification_list = [NotificationEventType.TABLE_ALTERED,
                                                                        NotificationEventType.TABLE_DROPPED],
                                                        terminate_on_table_altered=True,
                                                        terminate_on_connection_lost=True,
                                                        check_gpudb_and_table_state_counter=500,
                                                        decode_failure_threshold=5,
                                                        ha_check_threshold=10,
                                                        zmq_polling_interval=1000,
                                                        table_monitor_topic_id_list=["oEdnBcnFw5xArIPbpxm9tA==",
                                                                                    "0qpcEpoMR+x7tBNDZ4lMhg==",
                                                                                    "PBvWoh0Dcmz8nr3ce8zW3w=="]
                                                ))

        """

        __operation_list = "operation_list"
        __notification_list = "notification_list"
        __terminate_on_table_altered = 'terminate_on_table_altered'
        __terminate_on_connection_lost = 'terminate_on_connection_lost'
        __check_gpudb_and_table_state_counter = 'check_gpudb_and_table_state_counter'
        __decode_failure_threshold = 'decode_failure_threshold'
        __ha_check_threshold = 'ha_check_threshold'
        __zmq_polling_interval = 'zmq_polling_interval'
        __table_monitor_topic_id_list = "table_monitor_topic_id_list"

        _supported_options = [
            __operation_list,
            __notification_list,
            __terminate_on_table_altered,
            __terminate_on_connection_lost,
            __check_gpudb_and_table_state_counter,
            __decode_failure_threshold,
            __ha_check_threshold,
            __zmq_polling_interval,
            __table_monitor_topic_id_list
        ]

        @staticmethod
        def default():
            """Create a default set of options for GPUdbTableMonitorBase

            Returns:
                GPUdbTableMonitorBase.Options instance

            """
            return GPUdbTableMonitorBase.Options()

        def __init__(self, _dict=None):
            """ Constructor for GPUdbTableMonitorBase.Options class

            Parameters:
                _dict (dict)
                    Optional dictionary with options already loaded. Value can
                    be None; if it is None suitable sensible defaults will be
                    set internally.

            Returns:
                A GPUdbTableMonitorOptions object.
            """
            # Set default values
            self._operation_list = [TableEventType.INSERT]
            self._notification_list = [NotificationEventType.TABLE_DROPPED]
            self._terminate_on_table_altered = True
            self._terminate_on_connection_lost = True
            self._check_gpudb_and_table_state_counter = 1
            self._decode_failure_threshold = 5  # In seconds
            self._ha_check_threshold = 10  # In seconds
            self._zmq_polling_interval = 1000  # In milli seconds
            self._table_monitor_topic_id_list = None

            if _dict is None:
                return  # nothing to do

            if not isinstance(_dict, dict):
                raise GPUdbException(
                    "Argument '_dict' must be a dict; given '%s'."
                    % type(_dict))

            # Else,_dict is a dict; extract options from within it
            # Check for invalid options
            unsupported_options = set(_dict.keys()).difference(
                self._supported_options)
            if unsupported_options:
                raise GPUdbException(
                    "Invalid options: %s" % unsupported_options)

            op_list = _dict[self.__operation_list]
            topic_id_list = _dict[self.__table_monitor_topic_id_list]
            notification_list = _dict[self.__notification_list]

            if (op_list is not None and isinstance(op_list, list) \
                and len(op_list) > 0) \
                    and (topic_id_list is not None and isinstance(topic_id_list,
                                                                  list) \
                         and len(topic_id_list) > 0):
                raise GPUdbException(
                    "Both operation_list and table_monitor_topic_id_list "
                    "cannot be specified in options ..")

            if topic_id_list is not None:
                if not isinstance(topic_id_list, list):
                    raise GPUdbException(
                        "Option 'table_monitor_topic_id_list' of "
                        "'_dict' "
                        "must be a list; given "
                        "'%s'."
                        % type(topic_id_list))
                else:
                    if len(topic_id_list) > 3:
                        raise GPUdbException(
                            "Option 'table_monitor_topic_id_list' "
                            "of '_dict' "
                            "must be a list of max 3 elements; given %s"
                            % len(topic_id_list))

            if op_list is not None:
                if not isinstance(op_list, list):
                    raise GPUdbException(
                        "Option 'operation_list' of "
                        "'_dict' "
                        "must be a list; given "
                        "'%s'."
                        % type(op_list))
                else:
                    if len(op_list) > 3:
                        raise GPUdbException(
                            "Option 'operation_list' "
                            "of '_dict' "
                            "must be a list of max 3 elements; given %s"
                            % len(op_list))

            if notification_list is not None:
                if not isinstance(notification_list, list):
                    raise GPUdbException(
                        "Option 'notification_list' of "
                        "'_dict' "
                        "must be a list; given "
                        "'%s'."
                        % type(notification_list))
                else:
                    if len(notification_list) > 3:
                        raise GPUdbException(
                            "Option 'notification_list' "
                            "of '_dict' "
                            "must be a list of max 3 elements; given %s"
                            % len(notification_list))
            # Extract and save each option
            for (key, val) in _dict.items():
                setattr(self, key, val)

        # end __init__

        @property
        def decode_failure_threshold(self):
            """This is the getter for the property 'decode_failure_threshold'
            which can be used to set a time in seconds for the program to retry
            decoding a payload received from the table monitor in the server.

            Once this retry threshold is crossed and it still results in a
            failure an exception will be raised.

            Default value is 5 seconds.

            Returns: The value of this property in the Options class instance,
            which is an integer type value.

            """
            return self._decode_failure_threshold

        @decode_failure_threshold.setter
        def decode_failure_threshold(self, val):
            """This is a setter for the property decode_failure_threshold.

            Args:
                val (int): The positive integer value indicating the number of
                seconds.
            """
            try:
                value = int(val)
            except:
                raise GPUdbException(
                    "Property 'cluster_reconnect_count' must be numeric; "
                    "given {}".format(str(type(val))))

            # Must be > 0
            if (value <= 0):
                raise GPUdbException(
                    "Property 'decode_failure_threshold' must be "
                    "greater than 0; given {}".format(str(value)))

            self._decode_failure_threshold = val

        @property
        def ha_check_threshold(self):
            """This is the getter for the property 'ha_check_threshold'
            which can be used to set a time in seconds for the program to retry
            checking for HA switchover in case the primary host has dropped out
            of the cluster for some reason.

            Once this threshold is crossed and it still results in a
            failure an exception will be raised.

            Default value is 10 seconds.

            Returns: The value of this property in the Options class instance,
            which is an integer type value.

            """
            return self._ha_check_threshold

        @ha_check_threshold.setter
        def ha_check_threshold(self, val):
            """This is a setter for the property 'ha_check_threshold'

            Args:
                val (int): The positive integer value indicating the number of
                seconds.
            """
            try:
                value = int(val)
            except:
                raise GPUdbException(
                    "Property 'ha_check_threshold' must be numeric; "
                    "given {}".format(str(type(val))))

            # Must be > 0
            if (value <= 0):
                raise GPUdbException("Property 'ha_check_threshold' must be "
                                     "greater than 0; given {}".format(
                    str(value)))

            self._ha_check_threshold = val

        @property
        def terminate_on_connection_lost(self):
            """This is the getter for the property 'terminate_on_connection_lost'.
            This property indicates some kind of loss of connection to GPUdb
            due to network outage or some other possible causes.

            Default value is True.

            Returns: The value of this property in the Options instance, which
            is a boolean type.

            """
            return self._terminate_on_connection_lost

        @terminate_on_connection_lost.setter
        def terminate_on_connection_lost(self, val):
            """This is the setter for the property 'terminate_on_connection_lost'

            Args:
                val (bool): This is a boolean value, permissible values are
                True/False
            """
            if not isinstance(val, bool):
                raise GPUdbException(
                    "Property 'terminate_on_connection_lost' must be "
                    "boolean; given '{}' type {}"
                    "".format(val, str(type(val))))
            self._terminate_on_connection_lost = val

        @property
        def check_gpudb_and_table_state_counter(self):
            """This is the getter for the property
            'check_gpudb_and_table_state_counter'. If there is no message
            received from the poll on ZMQ socket, then this counter will be
            checked for the set value. If the counter has reached the value, it
            will check for HA switchover and reset the counter to 0.

            This is an integer value and the default is set to 500.

            Returns: The value of the counter as set in the Options instance.

            """
            return self._check_gpudb_and_table_state_counter

        @check_gpudb_and_table_state_counter.setter
        def check_gpudb_and_table_state_counter(self, val):
            """This is the setter for the property 'check_gpudb_and_table_state_counter'.

            Args:
                val (int): The positive integer value indicating the number of
                times the counter must be incremented for the HA switchover
                check to be invoked once.
            """
            try:
                value = int(val)
            except:
                raise GPUdbException(
                    "Property 'check_gpudb_and_table_state_counter' must be numeric; "
                    "given {}".format(str(type(val))))

            # Must be > 0
            if (value <= 0):
                raise GPUdbException(
                    "Property 'check_gpudb_and_table_state_counter' must be "
                    "greater than 0; given {}".format(str(value)))

            self._check_gpudb_and_table_state_counter = val

        @property
        def terminate_on_table_altered(self):
            """This is the getter for the property 'terminate_on_table_altered'.
            This property indicates some kind of loss of connection to GPUdb
            due to network outage or some other possible causes.

            Default value is True.

            Returns: The value of this property in the Options instance, which
            is a boolean type.

            """
            return self._terminate_on_table_altered

        @terminate_on_table_altered.setter
        def terminate_on_table_altered(self, val):
            """This is the setter for the property 'terminate_on_table_altered'

            Args:
                val (bool): This is a boolean value, permissible values are
                True/False

            """
            if not isinstance(val, bool):
                raise GPUdbException(
                    "Property 'terminate_on_table_altered' must be "
                    "boolean; given '{}' type {}"
                    "".format(val, str(type(val))))
            self._terminate_on_table_altered = val

        @property
        def zmq_polling_interval(self):
            """This is the getter for the property 'zmq_polling_interval'.
            This is a positive integer value and is in milliseconds. It
            indicates the number of seconds to wait before polling the ZMQ
            socket for new events.

            Default value is 1000 milliseconds.

            Returns: The value of this property in the Options instance.

            """
            return self._zmq_polling_interval

        @zmq_polling_interval.setter
        def zmq_polling_interval(self, val):
            """This is the setter for the property 'zmq_polling_interval'

            Args:
                val (int): The value for ZMQ socket polling interval to be set
                in milliseconds.
            """
            try:
                value = int(val)
            except:
                raise GPUdbException(
                    "Property 'zmq_polling_interval' must be numeric; "
                    "given {}".format(str(type(val))))

            # Must be > 0
            if (value <= 0):
                raise GPUdbException("Property 'zmq_polling_interval' must be "
                                     "greater than 0; given {}".format(
                    str(value)))

            self._zmq_polling_interval = val

        @property
        def table_monitor_topic_id_list(self):
            """This is a getter for the property 'table_monitor_topic_id_list'.

            This is a list of strings indicating the topic_ids for existing
            table monitors. The list can contain a maximum of three string
            values one for each topic for insert, update and delete respectively.

            Default value is None.

            This property and 'operation_list' are mutually exclusive. If one is
            specified in an Options instance, the other necessarily has to be
            None.

            Returns: The value of this property in the Options instance.

            """
            return self._table_monitor_topic_id_list

        @table_monitor_topic_id_list.setter
        def table_monitor_topic_id_list(self, val):
            """This is the setter for the property 'table_monitor_topic_id_list'

            Args:
                val (list): The list containing the topic_ids to watch for by
                subscribing to ZMQ.
            """
            if val is not None and not isinstance(val, list):
                raise GPUdbException(
                    "Property 'table_monitor_topic_id_list' must be of type list; given {}".format(
                        type(val)))

            self._table_monitor_topic_id_list = val

        @property
        def operation_list(self):
            """This is a getter for the property 'operation_list'.

            This is a list containing the enum values of TableEventType (INSERT,
            UPDATE or DELETE). This list can contain a maximum of three
            elements.

            The default value is a single element list [TableEventType.INSERT]

            Returns: The value of this property in an Options instance.

            """
            return self._operation_list

        @operation_list.setter
        def operation_list(self, val):
            """This is the setter for the property 'operation_list'

            Args:
                val (list): The list containing the operation enums to watch
                for, e.g., options.operation_list = [TableEventType.INSERT, TableEventType.DELETE]
            """
            if val is not None and not isinstance(val, list):
                raise GPUdbException(
                    "Property 'operation_list' must be of type list; given {}".format(
                        type(val)))

            self._operation_list = val

        @property
        def notification_list(self):
            """This is the getter for the property 'notification_list'.

            This is a list containing the Enum values of type
            NotificationEventType(TABLE_DROPPED, TABLE_ALTERED etc.).

            The default value is NotificationEventType.TABLE_DROPPED


            Returns: The value of this property in an Options instance.

            """
            return self._notification_list

        @notification_list.setter
        def notification_list(self, val):
            """This is the setter for the property 'notification_list'

            Args:
                val (list): The list containing the operation enums to watch
                for, e.g., options.notification_list = [NotificationEventType.TABLE_DROPPED]
            """
            if val is not None and not isinstance(val, list):
                raise GPUdbException(
                    "Property 'notification_list' must be of type list; given {}".format(
                        type(val)))

            self._notification_list = val

        def as_json(self):
            """Return the options as a JSON for using directly in create_table()"""
            result = {}
            if self.__operation_list is not None:
                result[self.__operation_list] = self._operation_list

            if self.__notification_list is not None:
                result[self.__notification_list] = self._notification_list

            if self._terminate_on_table_altered is not None:
                result[
                    self.__terminate_on_table_altered] = True if self._terminate_on_table_altered else False

            if self._terminate_on_connection_lost is not None:
                result[
                    self.__terminate_on_connection_lost] = True if self._terminate_on_connection_lost else False

            if self._check_gpudb_and_table_state_counter is not None:
                result[
                    self.__check_gpudb_and_table_state_counter] = self._check_gpudb_and_table_state_counter

            if self._decode_failure_threshold is not None:
                result[
                    self.__decode_failure_threshold] = self._decode_failure_threshold

            if self._ha_check_threshold is not None:
                result[self.__ha_check_threshold] = self._ha_check_threshold

            if self._zmq_polling_interval is not None:
                result[self.__zmq_polling_interval] = self._zmq_polling_interval

            if self._table_monitor_topic_id_list is not None:
                result[
                    self.__table_monitor_topic_id_list] = self._table_monitor_topic_id_list

            return result

        # end as_json

        def as_dict(self):
            """Return the options as a dict for using directly in create_table()"""
            return self.as_json()
        # end as_dict

    # End Options nested class

# End GPUdbTableMonitorBase class

class GPUdbTableMonitor(GPUdbTableMonitorBase):
    """ A default implementation which just passes on the received objects
        to the callbacks which are passed in as arguments to the constructor
        of this class.

        This class can be used as it is for simple requirements or more
        involved cases could directly inherit from GPUdbTableMonitor class and
        implement/override the callbacks to do further downstream processing.

        The default behaviour of these callback methods is to just log the
        events.

    """

    def __init__(self, db, tablename, options=None):
        """ Constructor for GPUdbTableMonitor class

        Args:
            db (GPUdb):
                The handle to the GPUdb

            tablename (str):
                Name of the table to create the monitor for

            options (GPUdbTableMonitor.Options):
                Options instance which is passed on to the super class
                GPUdbTableMonitor constructor
        """
        callbacks = GPUdbTableMonitorBase.Callbacks(
            cb_insert_raw=self.on_insert_raw,
            cb_insert_decoded=self.on_insert_decoded,
            cb_updated=self.on_update,
            cb_deleted=self.on_delete,
            cb_table_dropped=self.on_table_dropped
            )
        super(GPUdbTableMonitor, self).__init__(
            db, tablename,
            callbacks, options=options)

    def on_insert_raw(self, payload):
        """

        Args:
            payload:
        """
        table_event = TableEvent(TableEventType.INSERT,
                                 count=-1, record_list=list(payload))
        self.__logger.info("Payload received : %s " % str(table_event))

    def on_insert_decoded(self, payload):
        """

        Args:
            payload:
        """
        table_event = TableEvent(TableEventType.INSERT,
                                 count=-1, record_list=payload)
        self.__logger.info("Payload received : %s " % str(table_event))

    def on_update(self, count):
        """

        Args:
            count:
        """
        table_event = TableEvent(TableEventType.UPDATE, count=count)
        self.__logger.info("Update count : %s " % count)

    def on_delete(self, count):
        """

        Args:
            count:
        """
        table_event = TableEvent(TableEventType.DELETE, count=count)
        self.__logger.info("Delete count : %s " % count)

    def on_table_dropped(self, table_name):
        """
        Args:
            table_name:

        """
        notif_event = NotificationEvent(NotificationEventType.TABLE_DROPPED,
                                        table_name)
        self.__logger.error("Table %s dropped " % self.table_name)

# End GPUdbTableMonitor class
