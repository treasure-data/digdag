# bq>: Running Google BigQuery queries

**bq>** operator runs a query on Google BigQuery.

    _export:
      bq:
        dataset: my_dataset

    +step1:
      bq>: queries/step1.sql
    +step2:
      bq>: queries/step2.sql
      destination_table: result_table
    +step3:
      bq>: queries/step3.sql
      destination_table: other_project:other_dataset.other_table


Note: The **bq>** operator uses [standard SQL](https://cloud.google.com/bigquery/sql-reference/index) by default, whereas the default in the BigQuery console is [legacy SQL](https://cloud.google.com/bigquery/query-reference). To run *legacy* SQL queries, please set `use_legacy_sql: true`. For more information about *standard* SQL on BigQuery, see [Migrating from legacy SQL](https://cloud.google.com/bigquery/sql-reference/migrating-from-legacy-sql).

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **gcp.credential**: CREDENTIAL

  The [Google Cloud Platform account](https://cloud.google.com/docs/authentication#user_accounts_and_service_accounts) credential private key to use, in JSON format.

  For information on how to generate a service account key, see the [Google Cloud Platform Documentation](https://cloud.google.com/storage/docs/authentication#generating-a-private-key).

  Upload the private key JSON file to the digdag server using the `secrets` client command:

    digdag secrets --project my_project --set gcp.credential=@my-svc-account-b4df00d.json

## Options

* **bq>**: query.sql

  Path to a query template file. This file can contain `${...}` syntax to embed variables.

  Examples:

  ```
  bq>: queries/step1.sql
  ```

* **dataset**: NAME

  Specifies the default dataset to use in the query and in the `destination_table` parameter.

  Examples:

  ```
  dataset: my_dataset
  ```

  ```
  dataset: other_project:other_dataset
  ```

* **destination_table**: NAME

  Specifies a table to store the query results in.

  Examples:

  ```
  destination_table: my_result_table
  ```

  ```
  destination_table: some_dataset.some_table
  ```

  ```
  destination_table: some_project:some_dataset.some_table
  ```

  You can append a date as `$YYYYMMDD` form at the end of table name to store data in a specific partition.
  See [Creating and Updating Date-Partitioned Tables](https://cloud.google.com/bigquery/docs/creating-partitioned-tables) document for details.

  ```
  destination_table: some_dataset.some_partitioned_table$20160101
  ```

* **create_disposition**: CREATE_IF_NEEDED | CREATE_NEVER

  Specifies whether the destination table should be automatically created when executing the query.

  - `CREATE_IF_NEEDED`: *(default)* The destination table is created if it does not already exist.
  - `CREATE_NEVER`: The destination table must already exist, otherwise the query will fail.

  Examples:

  ```
  create_disposition: CREATE_IF_NEEDED
  ```

  ```
  create_disposition: CREATE_NEVER
  ```

* **write_disposition**: WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY

  Specifies whether to permit writing of data to an already existing destination table.

  - `WRITE_TRUNCATE`: If the destination table already exists, any data in it will be overwritten.
  - `WRITE_APPEND`: If the destination table already exists, any data in it will be appended to.
  - `WRITE_EMPTY`: *(default)* The query fails if the destination table already exists and is not empty.

  Examples:

  ```
  write_disposition: WRITE_TRUNCATE
  ```

  ```
  write_disposition: WRITE_APPEND
  ```

  ```
  write_disposition: WRITE_EMPTY
  ```

* **priority**: INTERACTIVE | BATCH

  Specifies the priority to use for this query. *Default*: `INTERACTIVE`.

* **use_query_cache**: BOOLEAN

  Whether to use BigQuery query result caching. *Default*: `true`.

* **allow_large_results**: BOOLEAN

  Whether to allow arbitrarily large result tables. Requires `destination_table` to be set and `use_legacy_sql` to be true.

* **flatten_results**: BOOLEAN

  Whether to flatten nested and repeated fields in the query results. *Default*: `true`. Requires `use_legacy_sql` to be true.

* **use_legacy_sql**: BOOLEAN

  Whether to use legacy BigQuery SQL. *Default*: `false`.

* **maximum_billing_tier**: INTEGER

  Limit the billing tier for this query. *Default*: The project default.

* **table_definitions**: OBJECT

  Describes external data sources that are accessed in the query. For more information see [BigQuery documentation](https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.tableDefinitions).

* **user_defined_function_resources**: LIST

  Describes user-defined function resources used in the query. For more information see [BigQuery documentation](https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.userDefinedFunctionResources).


## Output parameters

* **bq.last_job_id**

  The id of the BigQuery job that executed this query.

  Note: `bq.last_jobid` parameter is kept only for backward compatibility but you must not use it because it will be removed removed in a near future release.

