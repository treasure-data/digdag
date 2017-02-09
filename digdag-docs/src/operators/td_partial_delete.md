# td_partial_delete>: Delete range of Treasure Data table

**td_partial_delete>** operator deletes records from a Treasure Data table.

Please be aware that records imported using streaming import can't be deleted for several hours using td_partial_delete. Records imported by INSERT INTO, Data Connector, and bulk imports can be deleted immediately.

Time range needs to be hourly. Setting non-zero values to minutes or seconds will be rejected.

    +step1:
      td_partial_delete>:
      database: mydb
      table: mytable
      from: 2016-01-01 00:00:00 +0800
      to:   2016-02-01 00:00:00 +0800

## Secrets

* **td.apikey**: API_KEY
  The Treasure Data API key to use when running Treasure Data queries.

## Parameters

* **database**: NAME

  Name of the database.

  Examples:

  ```
  database: my_database
  ```

* **table**: NAME

  Name of the table to export.

  Examples:

  ```
  table: my_table
  ```

* **from**: yyyy-MM-dd HH:mm:ss[ Z]

  Delete records from this time (inclusive). Actual time range is :command:`[from, to)`. Value should be a UNIX timestamp integer (seconds) or string in yyyy-MM-dd HH:mm:ss[ Z] format.

  Examples:

  ```
  from: 2016-01-01 00:00:00 +0800
  ```

* **to**: yyyy-MM-dd HH:mm:ss[ Z]

  Delete records to this time (exclusive). Actual time range is :command:`[from, to)`. Value should be a UNIX timestamp integer (seconds) or string in yyyy-MM-dd HH:mm:ss[ Z] format.

  Examples:

  ```
  to: 2016-02-01 00:00:00 +0800
  ```

* **endpoint**: ADDRESS

  API endpoint (default: api.treasuredata.com).

* **use_ssl**: BOOLEAN

  Enable SSL (https) to access to the endpoint (default: true).

