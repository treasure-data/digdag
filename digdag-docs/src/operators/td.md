# td>: Treasure Data queries

**td>** operator runs a Hive or Presto query on Treasure Data.

    _export:
      td:
        database: www_access

    +simple_query:
      td>: queries/simple_query.sql

    +simple_query_expanded:
      td>:
      query: "SELECT '${session_id}' FROM nasdaq"

    +simple_query_nonexpanded:
      td>:
        data: "SELECT * FROM nasdaq"

    +create_new_table_using_result_of_select:
      td>: queries/select_sql.sql
      create_table: mytable_${session_date_compact}

    +insert_result_of_select_into_a_table:
      td>: queries/select_sql.sql
      insert_into: mytable

    +result_with_connection:
      td>: queries/select_sql.sql
      result_connection: connection_created_on_console

    +result_with_connection_with_settings:
      td>: queries/select_sql.sql
      result_connection: my_s3_connection
      result_settings:
        bucket: my_bucket
        path: /logs/

## Examples

  * [Examples](https://github.com/treasure-data/workflow-examples/tree/master/td)

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **td.apikey**: API_KEY

  The Treasure Data API key to use when running Treasure Data queries.

## Options

* **td>**: FILE.sql

  Path to a query template file. This file can contain `${...}` syntax to embed variables.

  Examples:

  ```
  td>: queries/step1.sql
  ```

* **data**: query

  A query can be passed as a string.
  (Note that this is actually not an option; this needs indent)

  Examples:

  ```
  td>:
    data: "SELECT * FROM nasdaq"
  ```

* **query**: query template

  A query template. This string can contain `${...}` syntax to embed variables.

  Examples:

  ```
  # Activate some customers to mail
  _export:
    database: project_a

  # This API returns following JSON, which has comments to track Presto Jobs and Workflows.
  # It contains a query whose condition is unfortunately dynamically decided...
  # ("dynamicaly" means it's not decided when they pushes this workflow)
  # {"query":"-- https://console.treasuredata.com/app/workflows/sessions/${session_id}\n-- ${task_name}\nSELECT * FROM customers WHERE ..."}
  +get_queries
    http>: http://example.com/get_queries
    store_content: true

  # doesn't expand given string
  +td_data:
    td>:
      data: ${JSON.parse(http.last_content)["query"]}
    result_connection: mail_something

  # it expands given string
  +td_query:
    td>:
    query: ${JSON.parse(http.last_content)["query"]}
    result_connection: mail_something
  ```

* **create_table**: NAME

  Name of a table to create from the results. This option deletes the table if it already exists.

  This option adds DROP TABLE IF EXISTS; CREATE TABLE AS (Presto) or INSERT OVERWRITE (Hive) commands before the SELECT statement. If the query includes a `-- DIGDAG_INSERT_LINE` line, the commands are inserted there.

  Examples:

  ```
  create_table: my_table
  ```

* **insert_into**: NAME

  Name of a table to append results into. The table is created if it does not already exist.

  This option adds INSERT INTO (Presto) or INSERT INTO TABLE (Hive) command at the beginning of SELECT statement. If the query includes `-- DIGDAG_INSERT_LINE` line, the command is inserted to the line.

  Examples:

  ```
  insert_into: my_table
  ```

* **download_file**: NAME

  Saves query result as a local CSV file.

  Examples:

  ```
  download_file: output.csv
  ```

* **store_last_results**: BOOLEAN

  Stores the first 1 row of the query results to `${td.last_results}` variable (default: false).
  td.last_results is a map of column name and a value. To access to a single value, you can use `${td.last_results.my_count}` syntax.

  Examples:

  ```
  store_last_results: true
  ```

* **preview**: BOOLEAN

  Tries to show some query results to confirm the results of a query.

  Examples:

  ```
  preview: true
  ```

* **result_url**: NAME
  Output the query results to the URL:

  Examples:

  ```
  result_url: tableau://username:password@my.tableauserver.com/?mode=replace
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

* **result_connection**: NAME

  Use a connection to write the query results to an external system.

  You can create a connection using the web console.

  Examples:

  ```
  result_connection: my_s3_connection
  ```

* **result_settings**: MAP

  Add additional settings to the result connection.

  This option is valid only if `result_connection` option is set.

  Examples:

  ```
  result_connection: my_s3_connection
  result_settings:
    bucket: my_s3_bucket
    path: /logs/
  ```

  ```
  result_connection: my_http
  result_settings:
    path: /endpoint
  ```

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

* **td.last_results**

  The first 1 row of the query results as a map. This is available only when `store_last_results: true` is set.

  Examples:

  ```
  {"path":"/index.html","count":1}
  ```

* **td.last_job.num_records**

  The number of records of this job output.
 
  Examples:
  
  ```
  10
  ```
