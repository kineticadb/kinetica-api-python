GPUdb Schemas Changelog
=======================


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
    -   /has/proc
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
