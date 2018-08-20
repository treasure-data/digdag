# td_result_export>: Treasure Data result exporter

**td_result_export>** operator exports job result to the specified result url

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

* **job_id**: JOB_ID

  The job_id whose job result is exported.
  
  You can also specify `${td.last_job_id}` as the last executed job id.

  Examples:

  ```
  job_id: 12345
  ```
  
* **result_url**: RESULT_URL

  The url where you want to export the result to.
  
  For all supported url style in Treasure Data, please see ["Writing Job Results into ***" in Treasure Data support site](https://support.treasuredata.com/hc/en-us/sections/360000245208-Databases).
  
  We **strongly** recommend using secrets to store all sensitive items(e.g. user, password, etc.). 

  Examples:

  ```
  result_url: mysql://${secret:user}:${secret:password}@${secret:host}/database/table
  ```
