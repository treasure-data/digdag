# td_result_export>: Treasure Data result exporter

**td_result_export>** operator exports a job result to an output destination.

    _export:
      td:
        database: www_access

    +simple_query:
      td>: queries/simple_query.sql

    +export_query_result:
      td_result_export>:
      job_id: 12345
      result_connection: my_s3_connection
      result_settings:
        bucket: my_bucket
        path: /logs/

## Options

* **job_id**: NUMBER The id of a job that is exported.

  Examples:

  ```
  job_id: 12345
  ```

  You can also specify `${td.last_job_id}` as the last executed job id.

  ```
  job_id: ${td.last_job_id}
  ```

* **result_connection**: NAME

  Use a connection to write the query results to an external system.

  You can create a connection using the web console.

  Examples:

  ```
  result_connection: my_s3_connection
  ```

* **result_settings**: MAP

  Add additional settings to the result connection.

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

  We **strongly** recommend using secrets to store all sensitive items (e.g. user, password, etc.) instead of writing down them in YAML files directly.
