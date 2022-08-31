<h3 align="center" style="margin:0px">
	<img width="200" src="https://2wz2rk1b7g6s3mm3mk3dj0lh-wpengine.netdna-ssl.com/wp-content/uploads/2018/08/kinetica_logo.svg" alt="Kinetica Logo"/>
</h3>
<h5 align="center" style="margin:0px">
	<a href="https://www.kinetica.com/">Website</a>
	|
	<a href="https://docs.kinetica.com/7.1/">Docs</a>
	|
	<a href="https://docs.kinetica.com/7.1/api/">API Docs</a>
	|
	<a href="https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg">Community Slack</a>   
</h5>


# Kinetica Python API

This is the 7.1.x.y version of the client-side Python API for Kinetica.  The
first two components of the client version must match that of the Kinetica
server.  When the versions do not match, the API will print a warning.  Often,
there are breaking changes between versions, so it is critical that they match.
For example, Kinetica 6.2 and 7.0 have incompatible changes, so the 6.2.x.y
versions of the Python API would NOT be compatible with 7.0.a.b versions.


## Contents

* [Installation Instructions](#installation-instructions)
* [Troubleshooting Installation](#troubleshooting-installation)
* [GPUdb Table Monitor Client API](#gpudb-table-monitor-client-api)
* [Support](#support)
* [Contact Us](#contact-us)


## Installation Instructions

To install this package, run `python setup.py install` in the root directory of
the repo.  Note that due to the in-house compiled C-module dependency, this
package must be installed, and simply copying `gpudb.py` or having a link to it
will not work.

There is also an example file in the example directory.

The documentation can be found at https://docs.kinetica.com/7.1/.  
The python specific documentation can be found at:

*   https://docs.kinetica.com/7.1/guides/python_guide/
*   https://docs.kinetica.com/7.1/api/python/


For changes to the client-side API, please refer to
[CHANGELOG.md](CHANGELOG.md).  For
changes to Kinetica functions, please refer to
[CHANGELOG-FUNCTIONS.md](CHANGELOG-FUNCTIONS.md).


### Troubleshooting Installation

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

## GPUdb Table Monitor Client API

A new API was introduced in version 7.0.17.0 to facilitate working with table
monitors, which watch for insert, update, and delete operations on a table.
The main classes to use are `GPUdbTableMonitor.Client` and
`GPUdbTableMonitor.Callback`.  `GPUdbTableMonitor.Client` creates and subscribes
to monitors on the target table per the user's choice; it can create one of each
type of the following monitors:

| Monitor Type | Triggered Event Result         |
|:-------------|:-------------------------------|
| Insert       | A list of the inserted records |
| Update       | Count of updated rows          |
| Delete       | Count of deleted rows          |

When one of the above events happen at the target table, the monitor client API
can invoke user-written functions that supposedly react to that type of event.
To facilitate integrating such user-written functions, `GPUdbTableMonitor.Callback`
is provided.  More on that to follow.
`GPUdbTableMonitor.Client` can be used in two ways:

*  Create an instance and pass in the appropriate arguments (including a list of
   `GPUdbTableMonitor.Callback` objects.  Use this instance in ther user's
   application directly.
   
*  Extend the class with all the necessary callback functions.  These newly
   definied functions then need to be passed to the superclass's, i.e.
   `GPUdbTableMonitor.Client`'s, constructor.
   
Note that the current implementation, the `GPUdbTableMonitor.Client` class
is designed to handle a single Kinetica table.
Also note that `GPUdbTableMonitor.Client` utilizes multiple threads internally.
This needs to be taken into consideration if the user application is also
multi-threaded.  There is an example of such a scenario included in the
`examples` directory (see the examples section below).


### GPUdbTableMonitor.Options

This class allows the user to configure the behavior of the
`GPUdbTableMonitor.Client` class.  The following options are currently available:

| Property Name             | Description | Default Value |
|:---                       | :---        | :---          |
| ``inactivity_timeout``    | A timeout in minutes for monitor inactivity. If the monitor does not receive any even triggers during such a period, the API will check if the table still exists or if the active Kinetica cluster is still operational.  The API will take appropriate corrective actions to ensure that the monitor continues to function.  In case of the deletion of the target table, the monitor will log the event and stop execution.  The parameter takes in float values to allow for fractions of minutes as the timeout. | 20 (minutes) |


#### GPUdbTableMonitor.Options Examples

```python
from gpudb import GPUdbTableMonitor
options = GPUdbTableMonitor.Options(
                                    _dict=dict(
                                    inactivity_timeout = 0.1,
                                ))
```

### GPUdbTableMonitor.Callback

This class facilitates integration of the table monitor API with the user's
client application code.  When the target table is monitored by
`GPUdbTableMonitor.Client` to have a triggering event like insertion, update, or
deletion of records, the client application needs to be notified.  There are
some additional events like the table being dropped or altered that also may
need to trigger actions in the user's application.  This class is the mechanism
for notifying the user application.  The notification is done via user-written
methods called callbacks that will be executed upon such trigger events; these
callbacks are passed to `GPUdbTableMonitor.Client` as a method reference via the
`GPUdbTableMonitor.Callback` class.
In other words, users pass methods that they have written via
`GPUdbTableMonitor.Callback` to `GPUdbTableMonitor.Client`, and the latter class
invokes these methods when trigger events occur.

### GPUdbTableMonitor.Callback.Type

Each `GPUdbTableMonitor.Callback` instance corresponds to a certain type of table
monitor event.  The `GPUdbTableMonitor.Callback.Type` enum represents which event
it is for.  The following are the currently available event types:

| Callback Type  |  Description |
|:---------------|:-------------|
| INSERT_DECODED | Describes a callback that is to be invoked when a record has been insserted into the target table; the API is to decode the record into a Python dict object and pass it to the callback. |
| INSERT_RAW     | Describes a callback that is to be invoked when a record has been insserted into the target table; the API will invoke the callback and pass the raw data (per record) to the method without any decoding. |
| DELETED        | Describes a callback that is to be invoked when records have been deleted from the target table; the API will pass the count of records deleted to the callback method. |
| UPDATED        | Describes a callback that is to be invoked when records have been update in the target table; the API will pass the count of updated records to the callback method. |
| TABLE_ALTERED  | Describes a callback that is to be invoked when the table has been altered in such a way that the record's structure type has been also changed. |
| TABLE_DROPPED  | Describes a callback that is to be invoked when the table has been dropped. |


### Callback Methods

Per callback type, there are two methods: one for the actual event (insert, update,
delete, alter, dropped etc.) and another for any error case.  The former is
called an `event callback`, and the latter `error callback`.


#### Event Callback

The event callback is a method that is written by the end-user of this API.  It
is passed by reference to `GPUdbTableMonitor.Callback`.  When the target table
has a trigger event, this method will be invoked by the table monitor API,
passing to it the value corresponding to the change that happened to the table.
The method must have a single input argument.  No return value is expected or
handled (therefore, the method could return something but it will simply be
ignored).  The actual name of the method does not matter at all.  Only the
signature--the sole input argument in this case--matters.  Here are the descriptions
of the method signature based on the table monitor type:

| Callback Type  | Input Argument Type | Input Argument Description |
|:---------------|:--------------------|:---------------------------|
| INSERT_DECODED | dict                | The record inserted into the target table decoded as a Python dict |
| INSERT_RAW     | bytes               | The record inserted into the target table in binary-encoded format |
| DELETED        | int                 | The number of records deleted from the target table. |
| UPDATED        | int                 | The number of records updated in the target table. |
| TABLE_ALTERED  | str                 | The name of the table. |
| TABLE_DROPPED  | str                 | The name of the table. |


#### Error Callback

`GPUdbTableMonitor.Callback` can take an optional method reference for error
cases.  If provided, this method will be called when errors occur during the
lifetime of the specific table monitor.  Note that since each
`GPUdbTableMonitor.Callback` instance takes in this optional method reference,
each type of table monitor event type can have its own specialized error
handling written by the user.  This method, like the event callback, needs to
have a single input argument.  The data type of this argument is string.  An
error message is passed to the method describing the error.  Like the event
callback, the return value of the method is ignored.


### GPUdbTableMonitor.Callback.Options

Each `GPUdbTableMonitor.Callback` object can have specialized options.  Note
that `GPUdbTableMonitor.Callback.Options` is not supposed to be passed to the
`GPUdbTableMonitor.Callback` constructor, but one its derived classes ought
to be passed in (each derived class pertains to a certain callback type).
Currently, only the `GPUdbTableMonitor.Callback.Type.INSERT_DECODED` has
meaningful options; therefore, only one class,
`GPUdbTableMonitor.Callback.InsertDecodedOptions` is defined.

#### GPUdbTableMonitor.Callback.InsertDecodedOptions

The following options are available for callbacks of type
`GPUdbTableMonitor.Callback.Type.INSERT_DECODED`:

| Property Name            | Description | Default Value |
|:---                      | :---        | :---          |
| ``decode_failure_mode``  | Indicates how the table monitor API should behave upon failures when trying to decode inserted records.  Upon a failure, the API will automatically try to recover once by checking if the table's type has been altered; if so, the API will retry decoding the record with the current type of the table.  If that succeeds, then the API continues.  However, if this second attempt at decoding fails, then the API needs to know what to do next.  | GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode.SKIP |

See `GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode` below.

#### GPUdbTableMonitor.Callback.InsertDecodedOptions.DecodeFailureMode

An enum describing the various modes of behavior when a decoding failure occurs
for an insert table monitor.

| Mode       | Description|
|:---        | :---       |
| ``SKIP``   | Skip this record upon decoding failure and proceed with the monitoring activities. |
| ``ABORT``  | Abort all monitoring activities and quit the program. |


### Examples

1.  [table_monitor_example.py](examples/table_monitor_example.py)
    This example uses the class `GPUdbTableMonitorExample` which is derived from
    the class ``GPUdbTableMonitor.Client`` to demonstrate how to use
    the client class provided by Kinetica for first-time users. The defined
    callback methods in `GPUdbTableMonitorExample` just logs the event payloads.
    
2.  [table_monitor_example_queued_impl.py](./examples/table_monitor_example_queued_impl.py)
    This example demonstrates a scenario where the table monitor API is used
    in an application that runs it's own thread(s).  In such a situation, some
    communication mechanism will be needed since the table monitor also runs
    its own separate threads.  To handle this inter-thread communication, a
    Queue instance is used.  There could be many ways to achieve the inter-thread
    communication; this is just an example to demonastrate such usage using the
    Python built-in Queue class.
    This example defines a class called `QueuedGPUdbTableMonitor` which inherits
    from `GPUdbTableMonitor.Client` and defins the callback functions.
    Additionally, this class has a Queue instance which is _shared with_ the
    client class `TableMonitorExampleClient`.  `TableMonitorExampleClient`
    inherits from Thread and runs in its own thread.  As the table monitor
    receives notifications it just pushes them into the shared Queue and then
    `TableMonitorExampleClient` consumes them from the shared Queue and
    displays them in the console.



## Support

For bugs, please submit an
[issue on Github](https://github.com/kineticadb/kinetica-api-python/issues).

For support, you can post on
[stackoverflow](https://stackoverflow.com/questions/tagged/kinetica) under the
``kinetica`` tag or
[Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg).


## Contact Us

* Ask a question on Slack:
  [Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg)
* Follow on GitHub:
  [Follow @kineticadb](https://github.com/kineticadb) 
* Email us:  <support@kinetica.com>
* Visit:  <https://www.kinetica.com/contact/>
