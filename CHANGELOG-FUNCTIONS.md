# Kinetica REST API Changelog

## Version 7.0

### Version 7.0.11.0 - 2019-12-10

#### Added Endpoints
- Added a new endpoint ``/insert/records/fromfiles`` to insert records from
  external files into a new or existing table.
- Added a new endpoint ``/modify/graph`` for updates of an existing graph
  in a non-streaming fashion.

#### Changed Endpoints

##### Non-breaking Changes
- Added an option ``remove_label_only`` to create and modify graph endpoints (see option's doc)
- Added ``enable_overlapped_equi_join`` and ``enable_compound_equi_join`` options to ``/alter/system/properties``
- Added ``columns`` and ``sql_where`` options to ``/grant/permission/table``

### Version 7.0.10.0 - 2019-11-13

#### Changed Endpoints

##### Breaking Changes
- Add ``additional_info`` map to ``/show/sql/proc`` to list attributes of sql procedures.

##### Non-breaking Changes
- Added ``allpaths`` solve graph option to solve for all the paths between source and destination.
  It is recommended to run this with the options of max and min solution radiua set carefully.
  The min value should be greater than or equal to the shortest path cost and it is further advised to set the
  max_number_targets option to limit the resulting paths.

- Added ``modify`` create graph option. When ``recreate`` and ``modify`` is both true for
  an existing graph it'll update the graph instead of deleting and recreating it.

- Added ``match_batch_solves`` solver type valid choice to the ``/match/graph``
  endpoint's ``solve_method`` parameter. This solver is strictly for WKTPOINT
  source and destination pairs with an ID and runs as many shortest paths as
  there are unique source and destination pairs provided as efficiently as
  possible. This change was made to bridge the gap for the ``shortest_path``
  solver in batch mode only for WKTPOINT node types in ``/solve/graph`` because
  it is not possible to match the ID of the pair in the solution_table.
- Added ``execute_interval`` and ``execute_start_time`` to the ``/show/sql/proc``.
- Added default values for parameters ``offset`` and ``limit`` to the following
  endpoints:
    - ``/aggregate/groupby``
    - ``/aggregate/unique``
    - ``/execute/sql``
    - ``/get/records/bycolumn``
- Changed default limit from 10k to -9999 (END_OF_SET) for the following endpoints:
    - ``/aggregate/groupby``
    - ``/aggregate/unique``
    - ``/execute/sql``
    - ``/get/records/bycolumn``
    - ``/get/records/fromcollection``
    - ``/get/records``
- Added "compact_after_rebalance" and "compact_only" options to /admin/rebalance


### Version 7.0.9.0 - 2019-10-16

#### Added Endpoints

- Added new ``/show/sql/proc`` endpoint to show SQL Procedure definitions

#### Changed Endpoints

##### Breaking Changes

- Updated the ``/solve/graph/`` endpoint to remove clunky and obscure primary parameters:
    - Removed ``source_node``, ``source_node_id``, ``destination_node_ids``, and ``node_type`` parameters
    - Added ``source_nodes`` parameter as vector of strings that is of NODE component type. For backwards compatibility, one of the following could be passed to both ``source_nodes`` and ``destination_nodes``:
        - ``["{'POINT(10 10)'} AS NODE_WKTPOINT"]``
        - ``["POINT(10 10)"]``
        - ``["table.column AS NODE_WKTPOINT"]``
- In the case of ``backhaul_routing`` solver type, the fixed assets are now placed in ``source_nodes`` and the remote assets are placed in ``destination_nodes`` instead of having all assets being placed in ``destination_nodes`` and the number of fixed assets being placed in ``source_node_id``

##### Non-breaking Changes

- Added ``bad_record_indices`` and ``error_N`` info map keys to ``/insert/records`` response
- Added ``evict_columns`` option to ``/alter/system/properties``
- Added ``return_individual_errors``, ``allow_partial_batch``, and ``dry_run`` options to ``/insert/records``
- Added ``retain_partitions`` option to ``/create/projection``. This option will cause the projection to be created with the same partition scheme as the source table, if possible. In prior versions, this was the default behavior but we are now making it optional and turned off by default.
- Added a ``max_combinations`` option to ``/match/graph`` for ``match_supply_demand`` solver type
- Added ``index_type`` option to ``/alter/table``. This option affects the ``create_index`` and ``delete_index`` actions and can have a value of ``column`` (the default) or ``chunk_skip``. Setting the option to ``column`` will create/delete a standard attribute index while the value of ``chunk_skip`` will create/delete a chunk-skip index, which is useful in cases where there are large numbers of chunks (i.e. due to partitioning).

### Version 7.0.8.0 - 2019-09-18

#### Changed Endpoints

##### Non-breaking Changes

- Added ``truncate_strings`` option to ``/update/records``

### Version 7.0.7.0 - 2019-08-15

#### Changed Endpoints

##### Non-breaking Changes

- Added ``match_supply_demand`` solver type valid choice to the ``/match/graph`` endpoint's ``solve_method`` parameter
- Added ``partial_loading`` option to ``/match/graph``
- Added support for the ``ulong`` (unsigned long) property within the ``string`` type to ``/create/type``

### Version 7.0.6.0 - 2019-07-11

#### Added Endpoints

- Added internal ``/show/graph/grammar`` endpoint to be consumed by GAdmin
- Added internal ``/alter/graph`` endpoint to enable cancelling graph jobs
- Added the following endpoints to support proc (UDF) execute permissions:
    - ``/grant/permission/proc``
    - ``/revoke/permission/proc``

- Added ``/show/graph`` to show basic properties of one or all graphs on the graph server

#### Changed Endpoints

##### Breaking Changes

- Moved ``rings`` option to a top-level parameter in ``/query/graph``

##### Non-breaking Changes

- Added ``aggressiveness`` option to ``/admin/rebalance`` and ``/admin/remove/ranks``
- Added ``show_async_jobs`` option to ``/admin/show/jobs`` to enable showing completed async jobs as they are no longer shown by default

### Version 7.0.5.0 - 2019-06-25

#### Changed Endpoints

- Aligned ``/visualize/isochrone`` ``options`` parameter with the ``contour_options`` and ``solve_options`` parameters
- Added ``use_priority_queue_solvers`` option to ``/visualize/isochrone`` to accelerate solve times for small to medium underlying graphs

### Version 7.0.4.0 - 2019-06-05

### Version 7.0.3.0 - 2019-05-01

### Version 7.0.2.0 - 2019-04-10

### Version 7.0.1.0 - 2019-03-01

### Version 7.0.0.0 - 2018-06-28

#### Added Endpoints

- Added the following endpoints for resource management:

    - ``/create/resourcegroup``
    - ``/show/resourcegroups``
    - ``/alter/resourcegroup``
    - ``/delete/resourcegroup``
    - ``/alter/tier``

#### Changed Endpoints

##### Breaking Changes

- Updated ``/create/user/internal`` to handle resource group names

##### Non-breaking Changes

- Added ``info`` string-to-string map to the responses of all endpoints. This map will contain additional information that may vary from endpoint to endpoint
- Updated ``/show/security`` response to include resource group names
- Updated ``/create/user/internal`` response to handle resource group names

## Version 6.2

### Version 6.2.0.0 - 2018-02-11

#### Added Endpoints

- Added the following endpoints to submit synchronously running jobs and retrieve the results of the job:
    - ``/create/job``
    - ``/get/job``
- Added ``/create/materializedview`` endpoint

#### Changed Endpoints

##### Non-breaking Changes

- Added ``max_concurrency_per_node`` option to ``/create/proc``

## Version 6.1

### Version 6.1.0.0 - 2018-01-08

#### Added Endpoints

- Added the following geometry-specific endpoints:

    - ``/aggregate/minmax/geometry``
    - ``/filter/byarea/geometry``
    - ``/filter/bybox/geometry``
    - ``/filter/byradius/geometry``

#### Changed Endpoints

##### Breaking Changes

- The following endpoints no longer support operations on geometry columns:
    - ``/aggregate/minmax``
    - ``/filter/byarea``
    - ``/filter/bybox``
    - ``/filter/byradius``

##### Non-breaking Changes

- Added support for the ``wkt`` property within the ``string`` type to ``/create/type``
- Added support for column names other than "WKT" to ``/filter/bygeometry``

## Version 6.0

### Version 6.0.0.0 - 2017-01-24

#### Added Endpoints

- Added ``/has/proc`` endpoint to support external procs

#### Changed Endpoints

##### Breaking Changes

- Moved ``execution_mode`` option to a top-level parameter in ``/create/proc``
- Modified ``/execute/proc``
- Shuffled ``/create/projection`` parameter order

##### Non-breaking Changes

- Added ``timings`` to the ``/show/proc`` response
- The following supported values have been added to ``action`` in ``/alter/table``:
    - ``add_column``
    - ``delete_column``
    - ``change_column``
    - ``rename_table``
- The following options have been added to ``/alter/table``:
    - ``column_default_value``
    - ``column_properties``
    - ``column_type``
    - ``validate_change_column``
    - ``copy_values_from_column``
    - ``rename_column``
- Added support for the following properties to ``/create/type``:
    - ``decimal`` (base type ``string``)
    - ``date`` (base type ``string``)
    - ``time`` (base type ``string``)
- The following supported values have been added to ``mode`` in ``/create/union``:
    - ``union_all`` (default)
    - ``union``
    - ``union_distinct``
    - ``except``
    - ``intersect``
- The following options have been added to ``/create/projection``:
    - ``expression``
    - ``limit``

## Version 5.4

### Version 5.4.0.0 - 2016-11-30

#### Added Endpoints

- Added /create/projection endpoint for selecting a subset of columns (including derived columns) from a table into a new result table with optional sorting
- Added the following external proc support endpoints:
    - ``/create/proc``
    - ``/delete/proc``
    - ``/execute/proc`` (replaces Node.js-exclusive version)
    - ``/kill/proc``
    - ``/show/proc``
    - ``/show/proc/status``

#### Changed Endpoints

##### Non-breaking Changes

- Null values for nullable columns are now supported in ``/update/records``

## Version 5.2

### Version 5.2.0.0 - 2016-09-21

#### Added Endpoints

- Added the following security system endpoints:
    - ``/alter/user``
    - ``/create/role``
    - ``/create/user/external``
    - ``/create/user/internal``
    - ``/delete/role``
    - ``/delete/user``
    - ``/grant/permission/system``
    - ``/grant/permission/table``
    - ``/grant/role``
    - ``/revoke/permission/system``
    - ``/revoke/permission/table``
    - ``/revoke/role``
    - ``/show/security``
- Added the ``/create/union`` endpoint

#### Changed Endpoints

##### Breaking Changes

- ``/alter/table/properties`` merged into ``/alter/table``
- ``/show/table/properties`` merged into ``/show/table``
- ``/visualize/image/heatmap`` has additional style options and a simplified schema

##### Non-breaking Changes

- ``/get/records`` now shows if there are more records to retrieve
- Added ``percentile`` to the list of supported values for ``stats`` in ``/aggregate/statistics``
- Added support for changing the maximum request timeout time to ``/alter/system/properties``
- Added ``not_in_list`` to the list of supported values for the ``filter_mode`` option in ``/filter/bylist`` to enable inverse matching
- Added ``result_table`` option to ``/aggregate/groupby``
- Added support for ``arg_min``, ``arg_max``, and ``count_distinct`` aggregates to ``/aggregate/groupby``
- Added ``result_table`` option to ``/aggregate/unique``

## Version 5.1

### Version 5.1.0.0 - 2016-05-06

#### Changed Endpoints

##### Non-breaking Changes

- Added ``having`` option to ``/aggregate/groupby``

## Version 4.2

### Version 4.2.0.0 - 2016-04-11

#### Changed Endpoints

##### Breaking Changes

- Refactor schemas and integrate documentation into JSON schemas
