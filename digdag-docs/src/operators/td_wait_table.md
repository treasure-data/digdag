# td_wait_table>: Waits for data arriving at Treasure Data table

**td_wait_table>** operator checks a table periodically until it has certain number of records in a configured range. This is useful to wait execution of following tasks until some records are imported to a table.

    _export:
      td:
        database: www_access

    +wait:
      td_wait_table>: target_table

    +step1:
      td>: queries/use_records.sql

## Secrets

* **td.apikey**: API_KEY

  The Treasure Data API key to use when running Treasure Data queries.

## Options

* **td_wait_table>**: FILE.sql

  Name of a table.

  Examples:

  ```
  td_wait_table>: target_table
  ```

* **rows**: N

  Number of rows to wait (default: 0).

  Examples:

  ```
  rows: 10
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

* **presto_pool_name**: NAME

  Name of a resource pool to run the queries in.
  Applicable only when ``engine`` is ``presto``.

  Examples:

  ```
  presto_pool_name: poc
  ```

* **hive_pool_name**: NAME

  Name of a resource pool to run the queries in.
  Applicable only when ``engine`` is ``presto``.

  Examples:

  ```
  engine: hive
  hive_pool_name: poc
  ```
