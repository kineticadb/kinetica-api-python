GPUdb Schemas Changelog
=======================

Version 6.2.0 - 2018-02-11
--------------------------

-   New endpoints to submit ssynchronously running jobs and retrieve the results of the job
    - /create/job
    - /get/job
-   New  /create/materializedview endpoint
-   /create/proc has new option 'max_concurrency_per_node'
-   /

Version 6.1.0 - 2018-01-08
--------------------------

-   /create/type has new column property: wkt
-   Endpoints no longer support operations on geometry columns
    -   /aggregate/minmax
    -   /filter/byarea
    -   /filter/bybox
    -   /filter/byradius
-   New geometry-specific endpoints
    -   /aggregate/minmax/geometry
    -   /filter/byarea/geometry
    -   /filter/bybox/geometry
    -   /filter/byradius/geometry
-   /filter/bygeometry now supports column names other than "WKT"

	
Version 6.0.0 - 2017-01-24
--------------------------

-   /alter/table has new actions:
    -   add_column
    -   delete_column
    -   change_column
    -   rename_table
-   /alter/table now supports the following additional options:
    -   column_default_value
    -   column_properties
    -   column_type
    -   validate_change_column
    -   copy_values_from_column
    -   rename_column
-   /create/proc 'execution_mode' is now a top-level parameter
-   /create/type has new column properties:
    -   decimal
    -   date
    -   time
-   New /create/union modes:
    -   union_all (the default mode)
    -   union
    -   union_distinct
    -   except
    -   intersect
-   Modified the /execute/proc API
-   Shuffled /create/projection API parameter order.
-   Added new options to /create/projection:
    -   expression
    -   limit
-   /show/proc has new output parameter:
    -   timings
-   New external proc support endpoint:
    -   /has/proc


Version 5.4.0 - 2016-11-30
--------------------------

-   New endpoint: /create/projection for selecting a subset of columns
    (including derived columns) from a table into a new result table, including
    optional sorting.
-   /update/records now supports null values for nullable columns.
-   New external proc support endpoints:
    -   /create/proc
    -   /delete/proc
    -   /execute/proc (replaces previous nodejs-based version)
    -   /kill/proc
    -   /show/proc
    -   /show/proc/status


Version 5.2.0 - 2016-09-21
--------------------------

-   /get/records now shows if there are more records to get.
-   /alter/table/properties merged into /alter/table, removed properties.
-   /show/table/properties merged into /show/table, removed properties.
-   /aggregate/statistics now supports 'percentile'.
-   /alter/system/properties can change the max request timeout time.
-   /filter/bylist supports 'not_in_list' for inverting match.
-   /visualize/image/heatmap has new 'style_options' and simplify schema.
-   New security system endpoints:
    -   /alter/user
    -   /create/role
    -   /create/user/external
    -   /create/user/internal
    -   /delete/role
    -   /delete/user
    -   /grant/permission/system
    -   /grant/permission/table
    -   /grant/role
    -   /revoke/permission/system
    -   /revoke/permission/table
    -   /revoke/role
    -   /show/security
-   /aggregate/groupby supports 'result_table' option.
-   /aggregate/groupby supports 'arg_min', 'arg_max', and 'count_distinct' aggregates.
-   /aggregate/unique supports 'result_table' option.
-   New /create/union endpoint.


Version 5.1.0 - 2016-05-06
--------------------------

-   /aggregate/groupby now supports 'having clause.
-   /execute/proc added for running nodejs procedures.


Version 4.2.0 - 2016-04-11
--------------------------

-   Refactor schemas and integrate documentation into JSON schemas
