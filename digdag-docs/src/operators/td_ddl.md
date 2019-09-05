# td_ddl>: Treasure Data operations

**td_ddl>** operator runs an operational task on Treasure Data.

    _export:
      td:
        database: www_access

    +step1:
      td_ddl>:
      create_tables: ["my_table_${session_date_compact}"]
    +step2:
      td_ddl>:
      drop_tables: ["my_table_${session_date_compact}"]
    +step3:
      td_ddl>:
      empty_tables: ["my_table_${session_date_compact}"]
    +step4:
      td_ddl>:
      rename_tables: [{from: "my_table_${session_date_compact}", to: "my_table"}]


If you would like to specify a different database which is not declared with _export, you can specify the database name under the options as below.


    _export:
      td:
        database: test_db1

    +task1:
      td_ddl>:
      create_tables: [test_ddl1, test_ddl2]
      database: test_db2



## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **td.apikey:** API_KEY

  The Treasure Data API key to use when performing Treasure Data operations.

## Options

* **create_tables**: [ARRAY OF NAMES]

  Create new tables if not exists.

  Examples:

  ```
  create_tables: [my_table1, my_table2]
  ```


* **empty_tables**: [ARRAY OF NAME]

  Create new tables (drop it first if it exists).

  Examples:

  ```
  empty_tables: [my_table1, my_table2]
  ```


* **drop_tables**: [ARRAY OF NAMES]

  Drop tables if exists.

  Examples:

  ```
  drop_tables: [my_table1, my_table2]
  ```

* **rename_tables**: [ARRAY OF {to:, from:}]

  Rename a table to another name (override the destination table if it already exists).

  Examples:

  ```
  rename_tables: [{from: my_table1, to: my_table2}]
  ```

* **create_databases**: [ARRAY OF NAMES]

  Create new databases if not exists.

  Examples:

  ```
  create_databases: [my_database1, my_database2]
  ```

* **empty_databases**: [ARRAY OF NAME]

  Create new databases (drop it first if it exists).

  Examples:

  ```
  empty_databases: [my_database1, my_database2]
  ```

.. note::

    Database permissions for the restricted users are not inherited. You need to grant permission again after ran `empty_databases`.

* **drop_databases**: [ARRAY OF NAMES]

  Drop databases if exists.

  Examples:

  ```
  drop_databases: [my_database1, my_database2]
  ```

* **endpoint**: ADDRESS

  API endpoint (default: api.treasuredata.com).

* **use_ssl**: BOOLEAN

  Enable SSL (https) to access to the endpoint (default: true).

