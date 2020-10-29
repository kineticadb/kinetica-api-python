import threading
import types
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
import logging

# We'll need to do python 2 vs. 3 things in many places
IS_PYTHON_3 = (sys.version_info[0] >= 3)  # checking the major component
IS_PYTHON_2 = (sys.version_info[0] == 2)  # checking the major component
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


# -----------------------------------------------------------------


class GPUdbTableMonitor(object):

    class Client(object):
        """ This class is the main client side API class implementing most of the
            functionalities. Implementing table monitor functions means that this class
            creates the server side table monitors in Kinetica and also starts
            listening for different events on those monitors like inserts, deletes
            and updates. Once the notifications (inserted records, deletions and
            updates) are received this class will call the different callback
            methods passed in the constructor.

            The intended use of this class is to derive from this and define the
            different callback methods and pass the callback methods wrapped in
            different callback objects of the types defined by the class
            :class:`GPUdbTableMonitor.Callback.Type`. The callback objects thus
            created must be passed as a list to the constructor of this class.

            A usage that will suffice for most cases has been given in the
            'examples/table_monitor_example.py' file. The user is requested to
            go through the example to get a thorough understanding of how to use
            this class.

            The readme contains the relevant links to navigate directly to the
            examples provided.

        """

        def __init__(self, db, table_name, callback_list, options=None):
            """

            Args:
                db (GPUdb)
                    The GPUdb object which is created external to this
                    class and passed in to facilitate calling different methods of the
                    GPUdb API. This has to be pre-initialized and must have a valid
                    value. If this is uninitialized then the constructor would raise a
                    GPUdbException exception.

                table_name (str)
                    The name of the Kinetica table for
                    which a monitor is to be created. This must have a valid value and
                    cannot be an empty string. In case this parameter does not have a
                    valid value the constructor will raise a :class:`GPUdbException`
                    exception.

                callback_list (list(GPUdbTableMonitor.Callback))
                    List of :class:`GPUdbTableMonitor.Callback` objects

                options (GPUdbTableMonitor.Options)
                    The class to encapsulate the various options that can be passed
                    to a :class:`GPUdbTableMonitor.Client` object to alter the
                    behaviour of the monitor instance. The details are given in the
                    section of :class:`GPUdbTableMonitor.Options` class.
            """
            # super(GPUdbTableMonitor.Client, self).__init__()

            if not self.__check_params(db, table_name):
                raise GPUdbException(
                    "Both db and table_name need valid values ...")

            self.type_id = ""
            self.type_schema = ""
            self.db = db
            self.full_url = self.db.gpudb_full_url
            self.table_name = table_name
            self.task_list = list()

            self._set_of_callbacks = set()

            self._insert_decoded_callback = None
            self._insert_raw_callback = None
            self._deleted_callback = None
            self._updated_callback = None
            self._table_altered_callback = None
            self._table_dropped_callback = None

            no_callbacks = all(cb is None for cb in callback_list)
            if no_callbacks:
                raise GPUdbException("No callbacks defined ... cannot proceed")

            monitor_callbacks = any(
                (cb is not None)
                and (
                    cb.callback_type in GPUdbTableMonitor.Callback.Type.monitor_types()
                )
                and (cb.event_callback is not None)
                for cb in callback_list)

            if not monitor_callbacks:
                raise GPUdbException(
                    "No callbacks defined to create table monitors "
                    "... cannot proceed")

            check_callbacks = all([isinstance(func, GPUdbTableMonitor.Callback)
                                   for func in callback_list])

            self.__operation_list = set()

            # Setup the logger for this instance
            self._id = str(uuid.uuid4())

            # self._logger is kept protected since it is also accessed from
            # GPUtbTableMonitorBase derived classes.
            self._logger = logging.getLogger(
                "gpudb_table_monitor.GPUdbTableMonitorBase_instance_" + self._id)

            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s %(levelname)-8s {%("
                                          "funcName)s:%(lineno)d} %(message)s",
                                          "%Y-%m-%d %H:%M:%S")
            handler.setFormatter(formatter)

            self._logger.addHandler(handler)

            # Prevent logging statements from being duplicated
            self._logger.propagate = False

            if not check_callbacks:
                raise GPUdbException(
                    "callbacks must be of type : 'Callback'")
            else:
                self._set_of_callbacks = set(callback_list)

                # Parse the set of callbacks and populate the respective instance
                # variables
                for cb in self._set_of_callbacks:
                    if cb.callback_type == GPUdbTableMonitor.Callback.Type.INSERT_DECODED:
                        self._insert_decoded_callback = cb
                    elif cb.callback_type == GPUdbTableMonitor.Callback.Type.INSERT_RAW:
                        self._insert_raw_callback = cb
                    elif cb.callback_type == GPUdbTableMonitor.Callback.Type.UPDATED:
                        self._updated_callback = cb
                    elif cb.callback_type == GPUdbTableMonitor.Callback.Type.DELETED:
                        self._deleted_callback = cb
                    elif cb.callback_type == GPUdbTableMonitor.Callback.Type.TABLE_ALTERED:
                        self._table_altered_callback = cb
                    elif cb.callback_type == GPUdbTableMonitor.Callback.Type.TABLE_DROPPED:
                        self._table_dropped_callback = cb
                    else:
                        self._logger.error("Unrecognized callback type ... ")
                        raise GPUdbException(
                            "Unrecognized callback type passed ... cannot parse")

            self.__operation_list = self.__get_operation_list_from_callbacks()

            if ((self.__operation_list is None)
                    or (len(self.__operation_list) == 0)):
                raise GPUdbException(
                    "Cannot determine table monitors needed from "
                    "the callback objects passed in ...")

            if options is None:
                # This is the default, created internally
                self.options = GPUdbTableMonitor.Options.default()
            else:
                # User has passed in options, check everything for validity
                if isinstance(options, GPUdbTableMonitor.Options):
                    try:
                        self.options = options

                    except GPUdbException as ge:
                        self._logger.error(ge.message)
                        raise GPUdbException(ge)

                else:
                    raise GPUdbException(
                        "Passed in options is not of the expected "
                        "type: Expected "
                        "'Options' type")


        # End __init__ Client

        def __get_operation_list_from_callbacks(self):
            """Internal method to retrieve a set of _TableEvent objects which is
            used to create the table monitors.

            Returns: A set of _TableEvent (enum) type objects.

            """
            operation_list = set()

            for obj in self._set_of_callbacks:
                if obj.callback_type in GPUdbTableMonitor.Callback.Type.monitor_types():
                    if (obj.callback_type in
                            [GPUdbTableMonitor.Callback.Type.INSERT_RAW,
                             GPUdbTableMonitor.Callback.Type.INSERT_DECODED]):
                        operation_list.add(
                            GPUdbTableMonitor.Client._TableEvent.INSERT)

                    elif obj.callback_type == GPUdbTableMonitor.Callback.Type.DELETED:
                        operation_list.add(
                            GPUdbTableMonitor.Client._TableEvent.DELETE)
                    elif obj.callback_type == GPUdbTableMonitor.Callback.Type.UPDATED:
                        operation_list.add(
                            GPUdbTableMonitor.Client._TableEvent.UPDATE)
                    else:
                        self._logger.error("No callback object found of one of "
                                           "the types of"
                                           "[INSERT_RAW, INSERT_DECODED, DELETED"
                                           "or UPDATED] to create table monitor"
                                           "... No table monitor can be created")
                        raise GPUdbException("Error : No callback object found "
                                             "of one of the types of"
                                           "[INSERT_RAW, INSERT_DECODED, DELETED"
                                           "or UPDATED] to create table monitor"
                                           "... No table monitor can be created")

            return operation_list

        def __check_params(self, db, table_name):
            """ This method checks the parameters passed into the Client
            constructor for correctness.

            Checks for existence of the table as well.

            Args:
                db (GPUdb)
                    the GPUdb object needed to access the different APIs

                table_name (str)
                    Name of the table to create the monitor for.

            Returns: Returns True or False if both the arguments are correct or
            wrong respectively.

            """
            table_name_value_correct = False

            if ( ( db is None ) or ( not isinstance(db, GPUdb) ) ):
                return False

            if (table_name is not None
                    and isinstance(table_name, (basestring, unicode))
                    and len(table_name.strip()) > 0):
                try:
                    has_table_response = db.has_table(table_name,
                                                      options={})
                    if has_table_response.is_ok:
                        table_name_value_correct = has_table_response[
                            "table_exists"]
                except GPUdbException as gpe:
                    self._logger.error(gpe.message)

            return table_name_value_correct

        def start_monitor(self):
            """ Method to start the Table Monitor
                This the API called by the client to start the table monitor

                This method has to be called to activate the table monitors which
                have been created in accordance to the callback objects passed in
                the constructor, whether this class is instantiated directly or a
                class derived from 'Client' (:class:`GPUdbMonitor.Client`) is used.

                .. seealso: :meth:`stop_monitor`
            """
            for event_type in self.__operation_list:

                if event_type == GPUdbTableMonitor.Client._TableEvent.INSERT:
                    insert_task = _InsertWatcherTask(self.db,
                                                     self.table_name,
                                                     options=self.options,
                                                     callbacks=[
                                                         self._insert_raw_callback,
                                                         self._insert_decoded_callback,
                                                         self._table_dropped_callback,
                                                         self._table_altered_callback]
                                                     )
                    insert_task.setup()
                    insert_task.logging_level = self.logging_level
                    insert_task.start()
                    self.task_list.append(insert_task)
                elif event_type == GPUdbTableMonitor.Client._TableEvent.UPDATE:
                    update_task = _UpdateWatcherTask(self.db,
                                                     self.table_name,
                                                     options=self.options,
                                                     callbacks=[
                                                         self._updated_callback,
                                                         self._table_dropped_callback,
                                                         self._table_altered_callback]
                                                     )
                    update_task.setup()
                    update_task.logging_level = self.logging_level
                    update_task.start()
                    self.task_list.append(update_task)
                else:
                    delete_task = _DeleteWatcherTask(self.db,
                                                     self.table_name,
                                                     options=self.options,
                                                     callbacks=[
                                                         self._deleted_callback,
                                                         self._table_dropped_callback,
                                                         self._table_altered_callback]
                                                     )
                    delete_task.setup()
                    delete_task.logging_level = self.logging_level
                    delete_task.start()
                    self.task_list.append(delete_task)

        def stop_monitor(self):
            """ Method to Stop the table monitor.
                This API is called by the client to stop the table monitor.
                This has to be called to stop the table monitor which has been
                started by the call 'start_monitor'.

                Failure to call this method will produce unpredictable results
                since the table monitors running in the background will not be
                stopped and cleaned up properly.

                .. seealso: :meth:`.start_monitor`
            """
            for task in self.task_list:
                task.stop()
                task.join()

        @property
        def logging_level(self):
            return self._logger.level

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
                self._logger.setLevel(value)
            except (ValueError, TypeError, Exception) as ex:
                raise GPUdbException("Invalid log level: '{}'".format(str(ex)))



        class _TableEvent(enum.Enum):
            """ Enum for table monitor event types

            This is an internal enum used for two purposes:
            1. Generating an internal operation list by parsing the callbacks passed
            to the Client class. The operations used for creating
            the table monitors are INSERT, UPDATE and DELETE. The other two are
            used for callbacks related to dropped and altered table notifications.

            2. Create the required table monitor of the right type by traversing
            the operation list.

            This is not meant to be used by the users of this API.

            """
            INSERT = 1
            """
            int: Indicates an INSERT event has occurred
            """

            UPDATE = 2
            """
            int: Indicates an UPDATE event has occurred
            """

            DELETE = 3
            """
            int: Indicates a DELETE event has occurred
            """

            TABLE_ALTERED = 4
            """
            int: Indicates a table has been altered
            """

            TABLE_DROPPED = 5
            """
            int: Indicates a table has been dropped
            """

    # End Client class



    class Options(object):
        """
        Encapsulates the various options used to create a table monitor. The
        class is initialized with sensible defaults which can be overridden by
        the users of the class. The following options are supported :

        * **inactivity_timeout**
            This option controls the time interval to set the timeout to
            determine when the program would do idle time processing like checking
            for the table existence, server HA failover if needed etc.. It is
            specified in minutes as a float so that seconds can be accommodated
            as well. The default value is set to 20 minutes, which is
            converted internally to seconds.

        Example usage:
            options = GPUdbTableMonitor.Options(_dict=dict(
            inactivity_timeout=0.1
        )
        )

        """
        __inactivity_timeout = 'inactivity_timeout'
        __INACTIVITY_TIMEOUT_DEFAULT = 20 * 60 * 1000

        _supported_options = [
            __inactivity_timeout
        ]

        @staticmethod
        def default():
            """Create a default set of options for :class:`Client`

            Returns:
                Options instance

            """
            return GPUdbTableMonitor.Options()

        def __init__(self, _dict=None):
            """ Constructor for GPUdbTableMonitor.Options class

            Parameters:
                _dict (dict)
                    Optional dictionary with options already loaded. Value can
                    be None; if it is None suitable sensible defaults will be
                    set internally.

            Returns:
                A GPUdbTableMonitor.Options object.
            """
            # Set default values
            # Default is 0.1 minutes = 6 secs
            self._inactivity_timeout = self.__INACTIVITY_TIMEOUT_DEFAULT

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

            # Extract and save each option
            for (key, val) in _dict.items():
                setattr(self, key, val)

        # end __init__

        @property
        def inactivity_timeout(self):
            """This is the getter for the property 'inactivity_timeout'.
            This is specified in minutes as a float so that seconds can be
            accommodated. This indicates a timeout interval after which if no
            notification is received from the server table monitors, the program
            will check whether everything is alright, like whether the table is
            still there and in the process will automatically trigger HA
            failover if needed.

            The default value is set to 20 minutes converted to milliseconds.

            Returns: The value of the timeout as set in the Options instance.

            """
            return self._inactivity_timeout

        @inactivity_timeout.setter
        def inactivity_timeout(self, val):
            """This is the setter for the property 'inactivity_timeout'.

            Args:
                val (float): This value is in minutes and internally converted
                to float so that seconds can be accommodated easily. The default
                value is 20 minutes converted to milliseconds.
            """
            try:
                value = float(val)
            except:
                raise GPUdbException(
                    "Property 'inactivity_timeout' must be numeric; "
                    "given {}".format(str(type(val))))

            # Must be > 0
            if (value <= 0):
                raise GPUdbException(
                    "Property 'inactivity_timeout' must be "
                    "greater than 0; given {}".format(str(value)))

            # Convert the value to milliseconds
            self._inactivity_timeout = val * 60 * 1000

        def as_json(self):
            """Return the options as a JSON"""
            result = {}

            if self.__inactivity_timeout is not None:
                result[self.__inactivity_timeout] = self._inactivity_timeout

            return result

        # end as_json

        def as_dict(self):
            """Return the options as a dict """
            return self.as_json()
        # end as_dict

    # End Options class



    class Callback(object):
        """Use this class to indicate which type of table monitor is desired.

        When the :class:`GPUdbTableMonitor.Client` is constructed a list of
        objects of this class has to be supplied to the constructor of the class.

        If the list of callbacks is empty or the list does not contain at least one
        of the callbacks of types (:class:`GPUdbTableMonitor.Callback.Type`)
        'INSERT_DECODED', 'INSERT_RAW', 'DELETED' or 'UPDATED' no table monitor
        would be created internally and the program would raise an exception of
        type :class:`GPUdbException` and exit. So, a list of objects of this class
        is mandatory for the table monitor to function.

        An example of using this class and passing on to the constructor of the
        class :class:`GPUdbTableMonitor.Client` is as follows:

        class GPUdbTableMonitorExample(GPUdbTableMonitor.Client):

            def __init__(self, db, table_name, options=None):

                # Create the list of callbacks objects which are to be passed to the
                # 'GPUdbTableMonitor.Client' class constructor

                # This example shows only two callbacks being created so
                # only an insert type table monitor will be created. For other
                # types callback objects could be created similarly to receive
                # notifications about other events.
                callbacks = [
                    GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.INSERT_RAW,
                                              self.on_insert_raw,
                                              self.on_error),

                    GPUdbTableMonitor.Callback(GPUdbTableMonitor.Callback.Type.INSERT_DECODED,
                                              self.on_insert_decoded,
                                              self.on_error,
                                              GPUdbTableMonitor.Callback.InsertDecodedOptions( GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.ABORT ))

                ]

                # Invoke the base class constructor and pass in the list of callback
                # objects created earlier.  This invocation is mandatory for the table
                # monitor to be actually functional.
                super(GPUdbTableMonitorExample, self).__init__(
                    db, table_name,
                    callbacks, options=options)
        """

        def __init__(
                self,
                callback_type,
                event_callback,
                error_callback=None,
                event_options=None,
                ):
            """
            Constructor for this class.

            Args:
                callback_type (enum :class:`GPUdbTableMonitor.Callback.Type`)
                    This indicates the type of the table monitor this callback
                    will be used for.
                    It must be of the type
                    :class:`GPUdbTableMonitor.Callback.Type` enum.

                event_callback (method reference)
                    This is to be called for any event related to an operation
                    on a table like insert, update, delete etc. As soon as such
                    an event is observed this method will be called.

                    This method can have only one parameter. For different
                    table monitor events (callback_type/s) the parameter would
                    be different. The method name has got no significance as
                    long as the signature is as given below:

                        def method_name(param):
                            # param - Could be a (dict|bytes|int|str)
                            # depending on the :attr:`callback_type`
                            # Processing Code follows ....

                    The method thus defined does not return anything.

                    The following table describes the parameter types which
                    correspond to each of the 'callback_type's:
                    --------------------------------------------------------
                    |NO | callback_type  | param type
                    --------------------------------------------------------
                    |1. | INSERT_DECODED | type will be 'dict'.
                    |2. | INSERT_RAW     | type will be 'bytes'.
                    |3. | DELETED        | type will be 'int'.
                    |4. | UPDATED        | type will be 'int'.
                    |5. | TABLE_ALTERED  | type will be 'str'.
                    |6. | TABLE_DROPPED  | type will be 'str'.

                error_callback (method reference)
                    Optional parameter.

                    This will be called in case of any operational error that
                    typically could manifest in the form of some exception
                    (GPUdbException).

                    The name of the method does not matter. It must have only
                    one argument of type 'str'. The argument to this method
                    will contain information related to the error that
                    occurred; often details about any exception that was
                    raised.

                    The signature of this method has to be:
                        def method_name(param):
                            # param - str
                            # code here ...

                event_options (:class:`GPUdbTableMonitor.Callback.Options`)
                    Optional parameter.

                    Options applicable to a specific callback type, e.g.,
                    insert, delete, update etc. Right now, the only option
                    applicable is for the callback handling insertion of records
                    where the record information is decoded and sent to the
                    callback by the table monitor.
            """
            if isinstance(callback_type, GPUdbTableMonitor.Callback.Type):
                self.__type = callback_type
            else:
                raise GPUdbException(
                    "Argument type must be of type "
                    "Callback.Type enum ...")

            if not self.__check_whether_function( error_callback ):
                raise GPUdbException("'error_callback' passed in is not a "
                                     "valid method reference")

            if not self.__check_whether_function( event_callback ):
                raise GPUdbException("'event_callback' passed in is not a "
                                     "valid method reference")

            self.__event_callback = event_callback
            self.__error_callback = error_callback

            if event_options is not None and not isinstance(event_options, GPUdbTableMonitor.Callback.Options):
                raise GPUdbException("event_options must be of type class 'Options'"
                                     " or a derived class")
            else:
                self.__event_options = event_options

        # End of Callback.init

        @property
        def event_callback(self):
            """
            Getter for the __event_callback field
            Used to call the method pointed to once an event is received
            """
            return self.__event_callback

        @property
        def error_callback(self):
            """
            Getter for the __error_callback field
            Used to call the method pointed to in case of an error related to
            the callback_type of this class.
            """
            return self.__error_callback

        @property
        def callback_type(self):
            return self.__type

        @property
        def event_options(self):
            return self.__event_options

        def __check_whether_function(self, func):
            """ Tests whether the object passed in is actually a function or not

            Args:
                func (Callback.event_callback):

            Returns: True or False

            """
            return func is None or isinstance(func, (types.FunctionType,
                                                     types.BuiltinFunctionType,
                                                     types.MethodType,
                                                     types.BuiltinMethodType
                                                     )) \
                   or callable(func)

        class Options(object):
            """
            This class embodies the options for any given callback type.  The
            :classs:`GPUdbTableMonitor.Callback` constructor expects an instance
            of this class.  However, instead of using this class directly, the
            user is supposed to use an instance of one of its derived classes.
            Each derived class is specialized with options that pertain to a
            certain type of callback.

            Note that, currently, there is only one derived class as other
            callback types do not have special options at the moment.

            .. seealso:: :class:`GPUdbTableMonitor.Callback.InsertDecodedOptions`
            """
            pass

        # End of Options class


        class InsertDecodedOptions(Options):
            """
            Options used to control the behaviour if there is some kind of
            error occurs while receiving notifications about inserted records
            after decoding.
            """

            def __init__(self, decode_failure_mode=None):
                """Constructor for this class.

                Args:
                    decode_failure_mode (:class:`GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode`)
                        This is either SKIP or ABORT as described in the class
                        documentation.
                """
                if decode_failure_mode is None:
                    self.__decode_failure_mode = GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.SKIP
                    return

                if not isinstance(decode_failure_mode, GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode):
                    raise GPUdbException("error_mode must be of type "
                                         "InsertDecodedOptions.DecodeFailureMode enum (SKIP|ABORT)")
                else:
                    self.__decode_failure_mode = decode_failure_mode

            @property
            def decode_failure_mode(self):
                """Getter method
                Return the __decode_failure_mode value.
                """
                return self.__decode_failure_mode

            @decode_failure_mode.setter
            def decode_failure_mode(self, value):
                """Setter
                Only allowed values are
                1. GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.SKIP
                2. GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.ABORT

                .. seealso:: :class:`GPUdbTableMonitor.Callback.InsertDecodedOptions`
                """
                if ( not isinstance(value, int )
                        or not isinstance(value, GPUdbTableMonitor.Callback.InsertDecodedOptions)
                        or ( value not in
                        [GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.SKIP,
                         GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.ABORT] ) ):
                    raise GPUdbException("'decode_failure_mode' value must be one of [PUdbTableMonitorCallback.InsertDecodedOptions.DecodeFailureMode.SKIP, "
                                         "Callback.InsertDecodedOptions.DecodeFailureMode.ABORT]")
                else:
                    self.__decode_failure_mode = value

            class DecodeFailureMode(enum.Enum):
                """
                This enum is used to identify the two possible modes to handle any
                error that can occur while decoding the payload received from the
                server table monitors.
                In both the cases (SKIP and ABORT) it will try to recover once by
                default.
                """

                ABORT = 1
                """
                int: If there is some kind of decoding error and
                ABORT is specified then the the program aborts (quits with an
                exception)
                """

                SKIP = 2
                """
                int: if SKIP is specified then the program will skip to
                the next record and try to decode that. In SKIP mode, the record
                which has been skipped due to problem in decoding will appear as
                an error log.
                """
            # End of DecodeFailureMode enum class

        # End of InsertDecodedOptions class

        class Type(enum.Enum):
            """ Indicates that the callback is for insert/update/delete event for
            the target table.  The API will create a[n] insert/update/delete
            table monitor, and per event, will invoke the appropriate event
            callback method. [Optional based on context: "Upon receiving records
            that have been recently inserted into the target table, the table
            monitor will/will not decode the records and pass the binary/decoded
            records to the event callback method."]
            """

            INSERT_DECODED = 1
            """
            int: This mode indicates an interest in receiving records after decoding
            according to the table schema. This is used to create an insert monitor
            internally. The user will get the notification through the callback
            method pointed to by 'event_callback' property of the class 
            'GPUdbTableMonitor.Callback'. The inserted records will be returned
            as a dict as an argument to the `event_callback`.
            .. seealso:: :class:`GPUdbTableMonitor.Client` class documentation.
            """

            INSERT_RAW = 2
            """
            int: This mode indicates an interest in receiving records before decoding
            that is as raw bytes. This is used to create an insert monitor
            internally. The user will get the notification through the callback
            method pointed to by 'event_callback' property of the class 
            'GPUdbTableMonitor.Callback'. The inserted records will be returned
            as bytes as an argument to the 'event_callback'.
            .. seealso:: :class:`GPUdbTableMonitor.Client` class documentation.
            """

            DELETED = 3
            """
            int: This mode indicates an interest in receiving notification about the
            count of deleted records. This is used to create a delete monitor
            internally. The user will get the notification through the callback
            method pointed to by 'event_callback' property of the class 
            'GPUdbTableMonitor.Callback'.
            .. seealso:: :class:`GPUdbTableMonitor.Client` class documentation.
            """

            UPDATED = 4
            """
            int: This mode indicates an interest in receiving notification about the
            count of updated records. This is used to create an update monitor
            internally. The user will get the notification through the callback
            method pointed to by 'event_callback' property of the class 
            'GPUdbTableMonitor.Callback'.
            .. seealso:: :class:`GPUdbTableMonitor.Client` class documentation.
            """

            TABLE_ALTERED = 5
            """
            int: This mode indicates an interest in receiving notification about the
            possible table alterations while one or more table monitors (insert,
            update, delete) are monitoring a table. If this is supplied then the
            user will be notified using the callback pointed to by 
            'event_callback' property of the class 'GPUdbTableMonitor.Callback'.
            .. seealso:: :class:`GPUdbTableMonitor.Client` class documentation.
            """

            TABLE_DROPPED = 6
            """
            int: This mode indicates an interest in receiving notification about the
            possible table deletions while one or more table monitors (insert,
            update, delete) are monitoring a table. If this is supplied then the
            user will be notified using the callback pointed to by 
            'event_callback' property of the class 'GPUdbTableMonitor.Callback'.
            .. seealso:: :class:`GPUdbTableMonitor.Client` class documentation.
            """

            @staticmethod
            def event_types():
                """This method returns the list of all available types and it is
                called to validate the callback type supplied to the constructor
                of the Callback class.
                """
                return [GPUdbTableMonitor.Callback.Type.INSERT_RAW,
                        GPUdbTableMonitor.Callback.Type.INSERT_DECODED,
                        GPUdbTableMonitor.Callback.Type.DELETED,
                        GPUdbTableMonitor.Callback.Type.UPDATED,
                        GPUdbTableMonitor.Callback.Type.TABLE_ALTERED,
                        GPUdbTableMonitor.Callback.Type.TABLE_DROPPED]

            @staticmethod
            def monitor_types():
                """This method returns the list of all available types that could be
                relevant to the creation of a table monitor and it is
                called to validate the callback type supplied to the constructor
                of the Callback class.
                """
                return [GPUdbTableMonitor.Callback.Type.INSERT_RAW,
                        GPUdbTableMonitor.Callback.Type.INSERT_DECODED,
                        GPUdbTableMonitor.Callback.Type.DELETED,
                        GPUdbTableMonitor.Callback.Type.UPDATED]

    # End of Callback class



class _BaseTask(threading.Thread):
    """ This an internal class and not to be used by clients.
        This is the base Task class from which all other tasks are derived
        that run the specific monitors for insert, update and delete etc.
    """

    def __init__(self,
                 db,
                 table_name,
                 table_event,
                 table_dropped_callback=None,
                 table_altered_callback=None,
                 options=None,
                 id=None):

        """
        Constructor for _BaseTask class, generally will not be needed to be
        called directly, will be called by one of the subclasses
        :class:`_InsertWatcherTask`, :class:`_UpdateWatcherTask`
        or :class:`_DeleteWatcherTask`

        Args:

        db (GPUdb)
            Handle to GPUdb instance

        table_name (str)
            Name of the table to create the monitor for

        table_event (_TableEvent)
            Enum of :class:`GPUdbTableMonitor.Client._TableEvent`
            Indicates whether the monitor is an insert, delete or
            update monitor

        table_dropped_callback (method reference)
            Reference to method passed to handle notifications related to
            dropped table.
            .. seealso: :class:`GPUdbTableMonitor.Callback`,
                        :class:`GPUdbTableMonitor.Callback.Type`

        table_altered_callback (method reference)
            Reference to method passed to handle notifications related to
            an altered table.
            .. seealso: :class:`GPUdbTableMonitor.Callback`,
                        :class:`GPUdbTableMonitor.Callback.Type`

        options (Options)
            Options to configure Client
            .. seealso: :class:`GPUdbTableMonitor.Options`

        Raises:
            GPUdbException
        """

        super(_BaseTask, self).__init__()

        if ( ( db is None ) or ( not isinstance(db, GPUdb) ) ):
            raise GPUdbException("db must be of type GPUdb")
        self.db = db

        if ( ( table_name is None )
                or ( not isinstance(table_name,(basestring, unicode)) ) ):
            raise GPUdbException("table_name must be a string")
        self.table_name = table_name

        if ( ( table_event is None )
                or ( not isinstance(table_event, GPUdbTableMonitor.Client._TableEvent) ) ):
            raise GPUdbException("table_event must be of type enum _TableEvent")

        self.event_type = table_event

        self.id = id

        if not isinstance(options, GPUdbTableMonitor.Options):
            options = GPUdbTableMonitor.Options.default()

        self._options = options
        self._table_dropped_callback = table_dropped_callback
        self._table_altered_callback = table_altered_callback

        self.type_id = None
        self.type_schema = None
        self.topic_id = ""
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.kill = False
        self.zmq_url = 'tcp://' + self.db.host + ':9002'
        self.full_url = self.db.gpudb_full_url
        self.record_type = None

        # Setup the logger for this instance
        self._id = str(uuid.uuid4())

        # self._logger is kept protected since it is also accessed from the
        # _BaseTask derived classes
        self._logger = logging.getLogger(
            "gpudb_table_monitor.BaseTask_instance_" + self._id)

        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)-8s {%("
                                      "funcName)s:%(lineno)d} %(message)s",
                                      "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)

        self._logger.addHandler(handler)

        # Prevent logging statements from being duplicated
        self._logger.propagate = False

    # End __init__ _BaseTask

    def setup(self):
        """This method sets up the internal state variables of the
        Client object like type_id, type_schema and create the
        table monitor and connects to the server table monitor.

        """

        self.type_id, self.type_schema = self._get_type_and_schema_for_table()

        if ( ( self.type_schema is not None )
                and ( self.type_id is not None ) ):
            self.record_type = RecordType.from_type_schema(
                label="",
                type_schema=self.type_schema,
                properties={})
        else:
            raise GPUdbException(
                "Failed to retrieve type_schema and type_id for table {}".format(
                    self.table_name))

        if self._create_table_monitor(table_event=self.event_type):
            self._connect_to_topic(self.zmq_url, self.topic_id)

    # End setup _BaseTask

    def _create_table_monitor(self, table_event):
        """This method creates the table monitor for the table name passed
        in as a parameter to the constructor of the table monitor class. It
        also caches the topic_id and the table schema in instance variables
        so that decoding of the messages received could be done.

        Returns: True or False

        """
        try:
            if ( table_event == GPUdbTableMonitor.Client._TableEvent.DELETE ):
                retval = self.db.create_table_monitor(self.table_name,
                                                      options={
                                                          'event': 'delete'})
            elif ( table_event == GPUdbTableMonitor.Client._TableEvent.INSERT ):
                retval = self.db.create_table_monitor(self.table_name,
                                                      options={
                                                          'event': 'insert'})
            elif ( table_event == GPUdbTableMonitor.Client._TableEvent.UPDATE ):
                retval = self.db.create_table_monitor(self.table_name,
                                                      options={
                                                          'event': 'update'})
            else:
                raise GPUdbException("Invalid 'table_event' value .. cannot "
                                     "create table monitor")

            # Retain the topic ID, used in verifying the queued messages'
            # source and removing the table monitor at the end
            self.topic_id = retval["topic_id"]

            # Retain the type schema for decoding queued messages
            self.type_schema = retval["type_schema"]
            return True
        except GPUdbException as gpe:
            self._logger.error(gpe.message)
            return False

    # End __create_table_monitor _BaseTask

    def _remove_table_monitor(self):
        """ Remove the established table monitor, by topic ID
        """
        self.db.clear_table_monitor(self.topic_id)

    # End _remove_table_monitor _BaseTask

    def _connect_to_topic(self, table_monitor_queue_url, topic_id):

        """ Create a connection to the message queue published to by the
        table monitor, filtering messages for the specific topic ID returned
        by the table monitor creation call

        """
        # Connect to queue using specified table monitor URL and topic ID
        self._logger.debug("Starting...")
        self.socket.connect(table_monitor_queue_url)

        if sys.version_info[:3] > (3, 0):
            topicid = "".join(chr(x) for x in bytearray(topic_id, 'utf-8'))
        else:
            topicid = topic_id.decode('utf-8')

        self.socket.setsockopt_string(zmq.SUBSCRIBE, topicid)

        self._logger.debug(" Started!")

    # End _connect_to_topic _BaseTask

    def _disconnect_from_topic(self):
        """ This method closes the server socket and terminates the context
        """
        self._logger.debug(" Stopping...")
        self.socket.close()
        self.context.term()
        self._logger.debug(" Stopped!")

    # End _disconnect_from_topic _BaseTask

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
                self._logger.error("ZMQ connection error : %s" % zmqe.message)
                # Try to re-create the table monitor, resorting to HA
                if not self._check_state_on_inactivity_timeout_expiry():
                    self.kill = True
        # End of while loop

        # Clean up when this table monitor is stopped
        self._disconnect_from_topic()
        self._remove_table_monitor()

    # End run _BaseTask

    def stop(self):
        """ This is a private method which just terminates the background
        thread which subscribes to the server table monitor topic and receives
        messages from it.

        """
        self.kill = True

    # End stop _BaseTask

    def _check_state_on_inactivity_timeout_expiry(self):
        """This method checks for table existence and other sanity checks while
        the main message processing loop is idle because server on
        successful polling returned nothing.

        Returns: Nothing

        """
        self._logger.debug("In _check_state_on_inactivity_timeout_expiry ...")

        table_monitor_created = False

        try:
            # Check whether the table is still valid
            table_exists = self.db.has_table(self.table_name, options={})[
                'table_exists']

            current_full_url = self.full_url

            if table_exists:
                if ( current_full_url != self.db.gpudb_full_url ):
                    # HA taken over

                    # Cache the full_url value
                    self.full_url = self.db.gpudb_full_url
                    self._logger.warning("{} :: HA Switchover "
                                         "happened : Current_full_url = {} "
                                         "and "
                                         "new_gpudb_full_url = {}".format(
                        self.id, current_full_url, self.db.gpudb_full_url))

                    new_type_id, new_type_schema = self._get_type_and_schema_for_table()

                    self._logger.debug(
                        "Old type_id = {} : New type_id = {}".format(
                            self.type_id, new_type_id))

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

                    table_monitor_created = True
            else:
                self._quit_on_exception(
                    event_type=GPUdbTableMonitor.Client._TableEvent.TABLE_DROPPED,
                    message="Table %s does not "
                    "exist anymore ..."
                    % self.table_name)

        except GPUdbException as gpe:
            self._logger.error("GpuDb error : %s" % gpe.message)
        except Exception as e:
            self._quit_on_exception(event_type=None, message=str(e))

        return table_monitor_created

    # End _check_state_on_inactivity_timeout_expiry _BaseTask

    def _quit_on_exception(self, event_type=None, message=None):
        """ This method is invoked on an exception which could be difficult
        to recover from and then it will simply terminate the background
        thread and exit cleanly. It will also indicate the clients of the
        table monitor by placing a special object 'None' in the shared Queue
        so that the clients know that they should terminate as well and can
        exit gracefully.

        Args: message: The exact exception message that could be logged for
        further troubleshooting
        """
        if message is not None:
            self._logger.error(message)

        if ( ( event_type == GPUdbTableMonitor.Client._TableEvent.TABLE_DROPPED )
                and ( self._table_dropped_callback is not None ) ):
            self._table_dropped_callback.event_callback(message)

        if ( ( event_type == GPUdbTableMonitor.Client._TableEvent.TABLE_ALTERED )
                and ( self._table_altered_callback is not None ) ):
            self._table_altered_callback.event_callback(message)

        # Connection to GPUDb failed or some other GPUDb failure, might as
        # well quit
        self.stop()

    # End _quit_on_exception _BaseTask

    def _get_type_and_schema_for_table(self):
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
            self._logger.error(gpe.message)
            return None, None

    # End __get_type_and_schema_for_table _BaseTask

    def execute(self):
        """ This method does the job of executing the task. It calls in sequence
            _connect, start and _disconnect.
            _connect connects to the server socket and sets up everything
            start starts the background thread
            _disconnect drops the server socket connection.

            This is actually a template method where _connect and _disconnect are
            implemented by the derived classes.

        """
        self._connect()
        self.start()
        self._disconnect()

    # End execute _BaseTask

    def _connect(self):
        """ Implemented by the derived classes _InsertWatcherTask,
            _UpdateWatcherTask and _DeleteWatcherTask.

        """
        raise NotImplementedError(
            "Method '_connect' of '_BaseTask' must be overridden in the derived classes")

    # End _connect _BaseTask

    def _fetch_message(self):
        """ This method is called by the run method which polls the socket and calls
            the method _process_message for doing the actual processing.
            _process_message is once again overridden in the derived classes.

        """
        ret = self.socket.poll(self._options.inactivity_timeout)

        if ret != 0:
            self._logger.debug("Received message .. ")
            messages = self.socket.recv_multipart()
            self._process_message(messages)

        else:
            # ret==0, meaning nothing received from socket.
            # Process all the other cases here since there is no
            # message to be processed.
            self._check_state_on_inactivity_timeout_expiry()

    # End _fetch_message _BaseTask

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
            "Method '_process_message' of '_BaseTask' must be overridden in the derived classes")

    # End _process_message _BaseTask

    def _disconnect(self):
        """Implemented by the derived classes _InsertWatcherTask,
            _UpdateWatcherTask and _DeleteWatcherTask.

        """
        raise NotImplementedError(
            "Method '_disconnect' of '_BaseTask' must be overridden in the derived classes")

    # End _disconnect _BaseTask

    @property
    def logging_level(self):
        return self._logger.level

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
            self._logger.setLevel(value)
        except (ValueError, TypeError, Exception) as ex:
            raise GPUdbException("Invalid log level: '{}'".format(str(ex)))


# End class _BaseTask


class _InsertWatcherTask(_BaseTask):
    """ This is the class which handles only inserts and subsequent processing
        of the messages received as a result of notifications from server on
        insertions of new records into the table.

    """

    def __init__(self, db, table_name,
                 options=None, callbacks=None):
        """
        [summary]

        Args:
        db (GPUdb)
            Handle to GPUdb instance

        table_name (str)
            Name of the table to create the monitor for

        options (:class:`GPUdbTableMonitor.Options`)
            Options to configure Client

        callbacks (list of :class:`GPUdbTableMonitor.Callback`): List of
        Callback objects passed by user to be called on various events relevant
        to Insert operation
        Order of callbacks in the list:
            - 0 - insert_raw callback
            - 1 - insert_decoded callback
            - 2 - table_dropped callback
            - 3 - table_altered callback

        """
        table_event = GPUdbTableMonitor.Client._TableEvent.INSERT

        self._callbacks = None if callbacks is None else callbacks

        self.__cb_insert_raw = None if self._callbacks is None else self._callbacks[0]

        self.__cb_insert_decoded = None if self._callbacks is None else self._callbacks[1]

        super(_InsertWatcherTask, self).__init__(db,
                                                 table_name,
                                                 table_event=table_event,
                                                 table_dropped_callback=self._callbacks[2],
                                                 table_altered_callback=self._callbacks[3],
                                                 options=options,
                                                 id='INSERT_' + table_name)


    def _connect(self):
        """ Overrides the base class method
            wrapping the call to setup method.
        """
        self.setup()


    def _try_decoding_on_table_altered(self, message_data):
        """This method tries to decode with the new type schema in case a
            table has been altered. The method will retry forever and would
            only fail if the 'DECODE_FAILURE_THRESHOLD_SECS' seconds have
            elapsed and still the decoding of the message has failed.

        Args:
            message_data:
            The raw message data (binary)

        Returns:
            record:
            The decoded record if there is one else None

        """
        record = None

        try:
            # retry with refreshed type details id
            # and schema
            new_type_id, new_type_schema = self._get_type_and_schema_for_table()

            self.record_type = RecordType.from_type_schema(
                label="",
                type_schema=new_type_schema,
                properties={})

            record = dict(GPUdbRecord.decode_binary_data(self.record_type,
                                                         message_data)[0])

            # Update the instance variables on
            # success
            self.type_id, self.type_schema = new_type_id, new_type_schema

        except Exception as e:
            self._logger.error("Exception received "
                               "while decoding : "
                               "%s" % str(e))
            self._logger.error(
                "Failed to decode message %s with "
                "updated schema %s" % message_data,
                self.type_schema)

        return record

    # End _try_decoding_on_table_altered

    def _process_message(self, messages):
        """ Process only messages assuming that they are inserts.

        Args:
            messages (list): Multi-part messages received from a single socket
            poll.
        """

        if IS_PYTHON_2 :
            topic_id_recvd = "".join(
                chr(x) for x in bytearray(messages[0], 'utf-8'))
        else:
            topic_id_recvd = str(messages[0], 'utf-8')

        self._logger.info("Topic_id_received = " + topic_id_recvd)

        # Process all messages, skipping the (first) topic frame
        for message_index, message_data in enumerate(messages[1:]):

            if ( self.__cb_insert_raw is not None
                    and self.__cb_insert_raw.event_callback is not None ):
                try:
                    self.__cb_insert_raw.event_callback(message_data)
                except Exception as e:
                    self._logger.error(e)
                    raise GPUdbException(str(e))

            # Decode the record from the message using the type
            # schema, initially returned during table monitor
            # creation
            if ( self.__cb_insert_decoded is not None
                    and self.__cb_insert_decoded.event_callback is not None ):
                try:
                    record = dict(GPUdbRecord.decode_binary_data(self.record_type,
                                                             message_data)[0])
                    try:
                        self.__cb_insert_decoded.event_callback( record )
                    except Exception as cbe:
                        self._logger.error(cbe)
                        raise GPUdbException("Exception in calling event_callback"
                                             "for insert decoded : " + str(cbe))

                except Exception as e:
                    # The exception could only be because of some
                    # issue with decoding the data; possibly due to
                    # a different schema resulting from a table
                    # alteration.
                    self._logger.error(
                        "Exception received while decoding {}".format(str(e)))

                    # Attempt recovery once anyway
                    record = self._try_decoding_on_table_altered(
                        message_data)

                    if ( record is None ):
                        if ( self.__cb_insert_decoded.event_options.error_mode
                                == GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.ABORT):

                            self._logger.error("Failed to decode message {} "
                                               "with schema {}".format(
                                message_data,
                                self.type_schema))

                            if ( self.__cb_insert_decoded.error_callback is not None):
                                self.__cb_insert_decoded.error_callback("Failed to decode message {} "
                                               "with schema {}".format(
                                message_data,
                                self.type_schema))

                            self._quit_on_exception(GPUdbTableMonitor.Client._TableEvent.TABLE_ALTERED,
                                                    "Table altered, "
                                                    "terminating ..."
                                                    )
                        elif (self.__cb_insert_decoded.event_options.error_mode
                                == GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.SKIP):
                            # This is the case of DecodeFailureMode.SKIP
                            # Emit a waring log message and skip over to
                            # subsequent records.
                            self._logger.warning("Failed to decode message {} "
                                       "with schema {}, skipping to next "
                                                 "records.".format(message_data,
                                                               self.type_schema))
                            continue
                        else:
                            self._logger.error("Unknown 'DecodeFailureMode' found .. cannot handle")
                            raise GPUdbException("Unknown 'DecodeFailureMode' found .. cannot handle")
                    else:
                        # Decoded second time, send the notification
                        self.__cb_insert_decoded.event_callback( record )

                    # End if (not decoded)

    # End _process_message _InsertWatcherTask(_BaseTask)

    def _disconnect(self):
        """

        """
        self.stop()


class _UpdateWatcherTask(_BaseTask):
    """ This is the class which handles only updates and subsequent processing
        of the messages received as a result of notifications from server on
        updates to the records of a table.

    """

    def __init__(self, db, table_name,
                 options=None,
                 callbacks=None,
                 ):

        """
        Constructor the the class _UpdateWatcherTask which inherits from
        _BaseTask

        Args:
        db (GPUdb)
            Handle to GPUdb instance

        table_name (str)
            Name of the table to create the monitor for

        options (:class:`GPUdbTableMonitor.Options`)
            Options to configure Client

        callbacks (list of :class:`GPUdbTableMonitor.Callback`): List of
        Callback objects passed by user to be called on
        various events relevant to Update operation
            Order of callbacks in the list:
            - 0 - updated callback
            - 1 - table_dropped callback
            - 2 - table_altered callback
        """

        table_event = GPUdbTableMonitor.Client._TableEvent.UPDATE

        self._callbacks = None if ( callbacks is None
                                    or ( not isinstance(callbacks, list)
                                    or len(callbacks) <= 0 ) ) else callbacks

        self.__cb_update = self._callbacks[0]

        super(_UpdateWatcherTask, self).__init__(db,
                                                 table_name=table_name,
                                                 table_event=table_event,
                                                 table_dropped_callback=self._callbacks[1],
                                                 table_altered_callback=self._callbacks[2],
                                                 options=options,
                                                 id='UPDATE_' + table_name)


    def _connect(self):
        """ Overrides the base class method
            wrapping the call to setup method.
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

        # Process all messages, skipping the (first) topic frame

        # Decode the record from the message using the type
        # schema, initially returned during table monitor
        # creation
        if ( self.__cb_update is not None
                and self.__cb_update.event_callback is not None):
            try:
                returned_obj = dict(
                    GPUdbRecord.decode_binary_data(self.type_schema, messages[1])[
                        0])
                self.__cb_update.event_callback(returned_obj["count"])

                self._logger.debug("Topic Id = {} , record = {} "
                                   .format(topic_id_recvd,
                                           returned_obj["count"]))
            except Exception as e:
                # The exception could only be because of some
                # issue with decoding the data; possibly due to
                # a different schema resulting from a table
                # alteration.
                self._logger.error(
                    "Exception received while decoding {}".format(
                        str(e)))
                self._logger.error("Failed to decode message {} "
                                   "with schema {}".format(
                    messages[1],
                    self.type_schema
                ))

    # End _process_message _UpdateWatcherTask(_BaseTask)

    def _disconnect(self):
        """

        """
        self.stop()


class _DeleteWatcherTask(_BaseTask):
    """ This is the class which handles only deletes and subsequent processing
        of the messages received as a result of notifications from server on
        on deletions of records of a table.

    """

    def __init__(self, db, table_name,
                 options=None, callbacks=None):
        """
        Constructor of the _DeleteWatcherTask class

        Args:
        db (GPUdb) :
            Handle to GPUdb instance

        table_name (str):
            Name of the table to create the monitor for

        options (:class:`GPUdbTableMonitor.Options`):
            Options to configure Client

        callbacks (List of :class:`GPUdbTableMonitor.Callback`):
            List of Callback objects passed by user to be called on various
            events relevant to the Delete operation
            Order of callbacks in the list:
            - 0 - deleted callback
            - 1 - table_dropped callback
            - 2 - table_altered callback
        """

        table_event = GPUdbTableMonitor.Client._TableEvent.DELETE

        self._callbacks = ( None if callbacks is None
                                    or ( not isinstance(callbacks, list)
                                         or len(callbacks) <= 0 ) else callbacks )
        self.__cb_delete = None if self._callbacks is None else self._callbacks[0]

        super(_DeleteWatcherTask, self).__init__(db,
                                                 table_name=table_name,
                                                 table_event=table_event,
                                                 table_dropped_callback=self._callbacks[1],
                                                 table_altered_callback=self._callbacks[2],
                                                 options=options,
                                                 id='DELETE_' + table_name)


    def _connect(self):
        """ Overrides the base class method
            wrapping the call to setup method.
        """
        self.setup()

    def _process_message(self, messages):
        """

        """
        self._logger.debug("Messages  = %s" % messages)

        if sys.version_info[0] == 2:
            topic_id_recvd = "".join(
                chr(x) for x in bytearray(messages[0], 'utf-8'))
        else:
            topic_id_recvd = str(messages[0], 'utf-8')

        # Process all messages, skipping the (first) topic frame

        # Decode the record from the message using the type
        # schema, initially returned during table monitor
        # creation
        if ( self.__cb_delete is not None
                and self.__cb_delete.event_callback is not None ):
            try:
                retobj = dict(
                    GPUdbRecord.decode_binary_data(self.type_schema, messages[1])[
                        0])
                self.__cb_delete.event_callback(retobj["count"])

                self._logger.debug("Topic Id = {} , record = {} "
                                   .format(topic_id_recvd,
                                           retobj["count"]))
            except Exception as e:
                self._logger.error(
                    "Exception received while decoding {}".format(str(e)))

                self._logger.error("Failed to decode message {} "
                                   "with schema {}".format(messages[1],
                                                           self.type_schema))

    # End _process_message _DeleteWatcherTask(_BaseTask)

    def _disconnect(self):
        """

        """
        self.stop()


