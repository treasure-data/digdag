# td>: Treasure Data queries

**td>** operator runs a Hive or Presto query on Treasure Data.

    _export:
      td:
        database: www_access

    +step1:
      td>: queries/step1.sql
    +step2:
      td>: queries/step2.sql
      create_table: mytable_${session_date_compact}
    +step3:
      td>: queries/step2.sql
      insert_into: mytable

## Examples

  * [Examples](https://github.com/treasure-data/workflow-examples/tree/master/td)

## Secrets

* **td.apikey**: API_KEY

  The Treasure Data API key to use when running Treasure Data queries.

## Options

* **td>**: FILE.sql

  Path to a query template file. This file can contain `${...}` syntax to embed variables.

  Examples:

  ```
  td>: queries/step1.sql
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


## Output parameters

* **td.last_job_id**

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
