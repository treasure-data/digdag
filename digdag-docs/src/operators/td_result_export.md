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
        result_url: mysql://${secret:user}:${secret:password}@${secret:host}/database/table

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

* **result_url**: NAME Output the job result to the URL.

  Examples:

  ```
  result_url: mysql://${secret:user}:${secret:password}@${secret:host}/database/table
  ```
  
  For all supported URL-style results in Treasure Data, please see ["Writing Job Results into ***" in Treasure Data support site](https://support.treasuredata.com/hc/en-us/sections/360000245208-Databases).

  We **strongly** recommend using secrets to store all sensitive items (e.g. user, password, etc.) instead of writing down them in YAML files directly.

## Output parameters

* **td.last_job_id**
* **td.last_job.id**

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
