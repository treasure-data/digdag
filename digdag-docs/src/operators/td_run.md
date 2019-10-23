# td_run>: Treasure Data saved queries

**td_run>** operator runs a query saved on Treasure Data.

    _export:
      td:
        database: www_access

    +step1:
      td_run>: 12345
    +step2:
      td_run>: myquery2
      session_time: 2016-01-01T01:01:01+00:00

## Examples

  * [Examples](https://github.com/treasure-data/workflow-examples/tree/master/td_run).

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **td.apikey**: API_KEY
  The Treasure Data API key to use when running Treasure Data queries.

## Options

* **td_run>**: SAVED_QUERY_ID or SAVED_QUERY_NAME

  Runs saved query. If number was specified, it's considered as an ID of saved query. Otherwise it's considered as a name of a saved query.

  Examples:

  ```
  td_run>: 12345
  ```

  Examples:

  ```
  td_run>: my_query
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

* **endpoint**: ADDRESS

  API endpoint (default: api.treasuredata.com).

* **use_ssl**: BOOLEAN

  Enable SSL (https) to access to the endpoint (default: true).


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
