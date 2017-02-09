# td_for_each>: Repeat using Treasure Data queries

**td_for_each>** operator loops subtasks for each result rows of a Hive or Presto query on Treasure Data.

Subtasks set at `_do` section can reference results using ${td.each.COLUMN_NAME} syntax where COLUMN_NAME is a name of column.

For example, if you run a query `select email, name from users` and the query returns 3 rows, this operator runs subtasks 3 times with `${td.each.email}` and `${td.each.name}}` parameters.

    _export:
      td:
        database: www_access

    +for_each_users:
      td_for_each>: queries/users.sql
      _do:
        +show:
          echo>: found a user ${td.each.name} email ${td.each.email}

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
