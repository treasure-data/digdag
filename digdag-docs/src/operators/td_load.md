# td_load>: Treasure Data bulk loading

**td_load>** operator loads data from storages, databases, or services.

    +step1:
      td_load>: config/guessed.yml
      database: prod
      table: raw

## Examples

  * [Examples](https://github.com/treasure-data/workflow-examples/tree/master/td_load)

## Secrets

* **td.apikey**: API_KEY

  The Treasure Data API key to use when submitting Treasure Data bulk load jobs.

## Options

* **td_load>**: FILE.yml

  Path to a YAML template file. This configuration needs to be guessed using td command. If you saved DataConnector job on Treasure Data, you can use job name instead of YAML path.

  Examples:

  ```
  td_load>: imports/load.yml
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

* **td.last_job_id**

  The job id this task executed.

  Examples:

  ```
  52036074
  ```
