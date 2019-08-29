# Kinetica REST API Changelog


## Version 7.0

### Version 7.0.7.0 - 2019-08-15

#### Changes

- Added "match_supply_demand" valid choice to the /match/graph endpoint's solve_method parameter
- Added a new option "partial_loading" (default=true) to the /match/graph endpoint's options
- Added 'ulong' property for string type in api /create/type/request json file.


### Version 7.0.6.1 - 2019-08-05

#### Additions

- Added new no-docs endpoint /show/graph/grammar to be consumed by gadmin.
- Added new no-api endpoint /alter/graph, mainly for cancelling graph jobs 
  (more functionality will be added later)


### Version 7.0.6.0 - 2019-07-25

#### Additions

- New endpoints to support proc (UDF) execute permissions
  - /grant/permission/proc
  - /revoke/permission/proc
- Added new endpoint /show/graph that shows basic properties of one or all 
  graphs on the graph server

#### Changes

- /admin/show/jobs no longer shows the completed async jobs by default. Use the
  option "show_async_jobs" to include the completed jobs.
- Added new rebalance "aggressiveness" option to /admin/rebalance and 
  /admin/remove/ranks

#### Breaking Changes

- Moved the "rings" parameter from the "options" list to the top-level of the
  /query/graph endpoint


### Version 7.0.5.0 - 2019-06-25

#### Changes

- Aligned /visualize/isochrone options to be inline with the contour and solve ones
- Added an option to /visualize/isochrone for solving using the priority_queue
  option both for direct and inverse senses. This accelerates solve times for
  small to medium underlying graphs


### Version 7.0.4.0 - 2019-06-05

No changes.


### Version 7.0.0.0 - 2018-06-28

#### Additions

- New endpoints for resource management
  - /create/resourcegroup
  - /show/resourcegroups
  - /alter/resourcegroup
  - /delete/resourcegroup
  - /alter/tier

#### Changes

- Updated /create/user/internal request/response to handle resource group names
- Updated /show/security response to include resource group names

#### Breaking Changes

- Added an output parameter named 'info' (a string-to-string map) to the
  responses of all the endpoints.  This map will contain additional information
  that may vary from endpoint to endpoint


## Version 6.2.0.0 - 2018-02-11

#### Additions

- New endpoints to submit synchronously running jobs and retrieve the results of the job
  - /create/job
  - /get/job
- New /create/materializedview endpoint

#### Changes

- /create/proc has new option 'max_concurrency_per_node'


## Version 6.1.0 - 2018-01-08

#### Changes

- /create/type has new column property: wkt
- New geometry-specific endpoints
  - /aggregate/minmax/geometry
  - /filter/byarea/geometry
  - /filter/bybox/geometry
  - /filter/byradius/geometry
- /filter/bygeometry now supports column names other than "WKT"

#### Breaking Changes

- Endpoints no longer support operations on geometry columns
  - /aggregate/minmax
  - /filter/byarea
  - /filter/bybox
  - /filter/byradius


## Version 6.0.0 - 2017-01-24

#### Changes

- /alter/table has new actions:
  - add_column
  - delete_column
  - change_column
  - rename_table
- /alter/table now supports the following additional options:
  - column_default_value
  - column_properties
  - column_type
  - validate_change_column
  - copy_values_from_column
  - rename_column
- /create/type has new column properties:
  - decimal
  - date
  - time
- New /create/union modes:
  - union_all (the default mode)
  - union
  - union_distinct
  - except
  - intersect
- Modified the /execute/proc API
- Shuffled /create/projection API parameter order.
- Added new options to /create/projection:
  - expression
  - limit
- New external proc support endpoint:
  - /has/proc

#### Breaking Changes

- /create/proc 'execution_mode' is now a top-level parameter
- /show/proc has new output parameter:
  - timings


## Version 5.4.0 - 2016-11-30

#### Additions

- New endpoint: /create/projection for selecting a subset of columns
  (including derived columns) from a table into a new result table, including
  optional sorting.

#### Changes

- /update/records now supports null values for nullable columns.
- New external proc support endpoints:
  - /create/proc
  - /delete/proc
  - /execute/proc (replaces previous nodejs-based version)
  - /kill/proc
  - /show/proc
  - /show/proc/status


## Version 5.2.0 - 2016-09-21

#### Additions

- New security system endpoints:
  - /alter/user
  - /create/role
  - /create/user/external
  - /create/user/internal
  - /delete/role
  - /delete/user
  - /grant/permission/system
  - /grant/permission/table
  - /grant/role
  - /revoke/permission/system
  - /revoke/permission/table
  - /revoke/role
  - /show/security

#### Changes

- /get/records now shows if there are more records to get.
- /aggregate/statistics now supports 'percentile'.
- /alter/system/properties can change the max request timeout time.
- /filter/bylist supports 'not_in_list' for inverting match.
- /visualize/image/heatmap has new 'style_options' and simplify schema.
- /aggregate/groupby supports 'result_table' option.
- /aggregate/groupby supports 'arg_min', 'arg_max', and 'count_distinct' aggregates.
- /aggregate/unique supports 'result_table' option.
- New /create/union endpoint.

#### Breaking Changes

- /alter/table/properties merged into /alter/table, removed properties.
- /show/table/properties merged into /show/table, removed properties.


## Version 5.1.0 - 2016-05-06

#### Changes

- /aggregate/groupby now supports 'having' clause.
- /execute/proc added for running nodejs procedures.


## Version 4.2.0 - 2016-04-11

#### Changes

- Refactor schemas and integrate documentation into JSON schemas
