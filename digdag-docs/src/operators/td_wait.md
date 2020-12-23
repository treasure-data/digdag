# td_wait>: Waits for data arriving at Treasure Data table

**td_wait>** operator runs a query periodically until it returns true. This operator can use more complex query compared to [td_wait_table> operator](td_wait_table.html).

    _export:
      td:
        database: www_access

    +wait:
      td_wait>: queries/check_recent_record.sql

    +step1:
      td>: queries/use_records.sql

Example queries:

    select 1 from target_table where TD_TIME_RANGE(time, '${session_time}') limit 1

    select count(*) > 1000 from target_table where TD_TIME_RANGE(time, '${last_session_time}')

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **td.apikey**: API_KEY

  The Treasure Data API key to use when running Treasure Data queries.

## Options

* **td_wait>**: FILE.sql

  Path to a query template file. This file can contain `${...}` syntax to embed variables.

  Examples:

  ```
  td_wait>: queries/check_recent_record.sql
  ```

* **database**: NAME

  Name of a database.

  Examples:

  ```
  database: my_db
  ```

* **endpoint**: ADDRESS

  API endpoint (default: api.treasuredata.com).

* **use_ssl**: BOOLEAN

  Enable SSL (https) to access to the endpoint (default: true).

* **engine**: presto

  Query engine (`presto` or `hive`).

  Examples:

  ```
  engine: hive
  ```

  ```
  engine: presto
  ```

* **interval**: 30s

  Set Interval (default: 30s (30 second)).

* **priority**: 0

  Set Priority (From `-2` (VERY LOW) to `2` (VERY HIGH) , default: 0 (NORMAL)).

* **job_retry**: 0

  Set automatic job retry count (default: 0).

  We recommend that you not set retry count over 10. If the job is not succeessful less than 10 times retry, it needs some fix a cause of failure.

* **presto_pool_name**: NAME

  Name of a resource pool to run the queries in.
  Applicable only when ``engine`` is ``presto``.

  Examples:

  ```
  presto_pool_name: poc
  ```

* **hive_pool_name**: NAME

  Name of a resource pool to run the queries in.
  Applicable only when ``engine`` is ``hive``.

  Examples:

  ```
  engine: hive
  hive_pool_name: poc
  ```

* **engine_version**: NAME

  Specify engine version for Hive and Presto.

  Examples:

  ```
  engine: hive
  engine_version: stable
  ```

* **hive_engine_version**: NAME

  Specify engine version for Hive.
  This option overrides ``engine_version`` if ``engine`` is ``hive``.

  Examples:

  ```
  engine: hive
  hive_engine_version: stable
  ```

## Output parameters

* **td.last_job_id** or **td.last_job.id**

  The job id this task executed.

  Examples:

  ```
  52036074
  ```

* **td.last_job.num_records**

  The number of records of this job output.
 
  Examples:
  
  ```
  10
  ```
