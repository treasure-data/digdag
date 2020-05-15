# td_for_each>: Repeat using Treasure Data queries

**td_for_each>** operator loops subtasks for each result rows of a Hive or Presto query on Treasure Data.

Subtasks set at `_do` section can reference results using ${td.each.COLUMN_NAME} syntax where COLUMN_NAME is a name of column.

For example, if you run a query `select email, name from users` and the query returns 3 rows, this operator runs subtasks 3 times with `${td.each.email}` and `${td.each.name}` parameters.

    _export:
      td:
        database: www_access

    +for_each_users:
      td_for_each>: queries/users.sql
      _do:
        +show:
          echo>: found a user ${td.each.name} email ${td.each.email}

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **td.apikey**: API_KEY

  The Treasure Data API key to use when running Treasure Data queries.

## Options

* **td_for_each>**: FILE.sql

  Path to a query template file. This file can contain `${...}` syntax to embed variables.

  Examples:

  ```
  td>: queries/step1.sql
  ```

* **_parallel**: BOOLEAN

  Runs the repeating tasks in parallel.

  Examples:

  ```
  _parallel: true
  ```

* **_do**: TASKS

  Tasks to run.

  Examples:

  ```
  # Runs a single task
  _do:
    echo>: ${td.each}
  ```

  ```
  # Runs multiple tasks
  _do:
    +show_email:
      echo>: ${td.each.email}
    +show_name:
      echo>: ${td.each.name}
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

* **priority**: 0

  Set Priority (From `-2` (VERY LOW) to `2` (VERY HIGH) , default: 0 (NORMAL)).

* **job_retry**: 0

  Set automatic job retry count (default: 0).

  We recommend that you not set retry count over 10. If the job is not succeessful less than 10 times retry, it needs some fix a cause of failure.

* **presto_pool_name**: NAME

  Name of a resource pool to run the query in.
  Applicable only when ``engine`` is ``presto``.

  Examples:

  ```
  presto_pool_name: poc
  ```

* **hive_pool_name**: NAME

  Name of a resource pool to run the query in.
  Applicable only when ``engine`` is ``hive``.

  Examples:

  ```
  engine: hive
  pool_name: poc
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
