# td_load>: Treasure Data bulk loading

**td_load>** operator loads data from storages, databases, or services.

    +step1:
      td_load>: config/guessed.yml
      database: prod
      table: raw

## Examples

  * [Examples](https://github.com/treasure-data/workflow-examples/tree/master/td_load)

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **td.apikey**: API_KEY

  The Treasure Data API key to use when submitting Treasure Data bulk load jobs.

## Options

* **td_load>**: FILE.yml

  Path to a YAML template file. This configuration needs to be guessed using td command. If you saved DataConnector job on Treasure Data, you can use [Unique ID](https://support.treasuredata.com/hc/en-us/articles/360001474328-Reference-an-Input-Data-Transfer#Configuring%20your%20Unique%20ID%20Incremental%20Data%20Transfer) instead of YAML path.

  Examples:

  ```
  td_load>: imports/load.yml
  ```

  ```
  td_load>: unique_id
  ```

* **database**: NAME

  Name of the database load data to.

  Examples:

  ```
  database: my_database
  ```

* **table**: NAME

  Name of the table load data to.

  Examples:

  ```
  table: my_table
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

* **td.last_job.num_records**

  The number of records of this job output.
 
  Examples:
  
  ```
  10
  ```
