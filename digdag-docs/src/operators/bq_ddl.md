# bq_ddl>: Managing Google BigQuery Datasets and Tables

**bq_ddl>** operator can be used to create, delete and clear Google BigQuery Datasets and Tables.

    _export:
      bq:
        dataset: my_dataset

    +prepare:
      bq_ddl>:
      create_datasets:
        - my_dataset_${session_date_compact}
      empty_datasets:
        - my_dataset_${session_date_compact}
      delete_datasets:
        - my_dataset_${last_session_date_compact}
      create_tables:
        - my_table_${session_date_compact}
      empty_tables:
        - my_table_${session_date_compact}
      delete_tables:
        - my_table_${last_session_date_compact}


## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **gcp.credential**: CREDENTIAL

  See [gcp.credential](bq.html#secrets).

## Options

* **create_datasets**: LIST

  Create new datasets.

  For detailed information about dataset configuration parameters, see the [Google BigQuery Datasets Documentation](https://cloud.google.com/bigquery/docs/reference/v2/datasets#resource).

  Examples:

  ```
  create_datasets:
    - foo
    - other_project:bar
  ```

  ```
  create_datasets:
    - foo_dataset_${session_date_compact}
    - id: bar_dataset_${session_date_compact}
      project: other_project
      friendly_name: Bar dataset ${session_date_compact}
      description: Bar dataset for ${session_date}
      default_table_expiration: 7d
      location: EU
      labels:
        foo: bar
        quux: 17
      access:
        - domain: example.com
          role: READER
        - userByEmail: ingest@example.com
          role: WRITER
        - groupByEmail: administrators@example.com
          role: OWNER
  ```

* **empty_datasets**: LIST

  Create new datasets, deleting them first if they already exist. Any tables in the datasets will also be deleted.

  For detailed information about dataset configuration parameters, see the [Google BigQuery Datasets Documentation](https://cloud.google.com/bigquery/docs/reference/v2/datasets#resource).

  Examples:

  ```
  empty_datasets:
    - foo
    - other_project:bar
  ```

  ```
  empty_datasets:
    - foo_dataset_${session_date_compact}
    - id: bar_dataset_${session_date_compact}
      project: other_project
      friendly_name: Bar dataset ${session_date_compact}
      description: Bar dataset for ${session_date}
      default_table_expiration: 7d
      location: EU
      labels:
        foo: bar
        quux: 17
      access:
        - domain: example.com
          role: READER
        - userByEmail: ingest@example.com
          role: WRITER
        - groupByEmail: administrators@example.com
          role: OWNER
  ```

* **delete_datasets**: LIST

  Delete datasets, if they exist.

  Examples:

  ```
  delete_datasets:
    - foo
    - other_project:bar
  ```

  ```
  delete_datasets:
    - foo_dataset_${last_session_date_compact}
    - other_project:bar_dataset_${last_session_date_compact}
  ```

* **create_tables**: LIST

  Create new tables.

  For detailed information about table configuration parameters, see the [Google BigQuery Tables Documentation](https://cloud.google.com/bigquery/docs/reference/v2/tables#resource).

  Examples:

  ```
  create_tables:
    - foo
    - other_dataset.bar
    - other_project:yet_another_dataset.baz
  ```

  ```
  create_tables:
    - foo_dataset_${session_date_compact}
    - id: bar_dataset_${session_date_compact}
      project: other_project
      dataset: other_dataset
      friendly_name: Bar dataset ${session_date_compact}
      description: Bar dataset for ${session_date}
      expiration_time: 2016-11-01-T01:02:03Z
      schema:
        fields:
          - {name: foo, type: STRING}
          - {name: bar, type: INTEGER}
      labels:
        foo: bar
        quux: 17
      access:
        - domain: example.com
          role: READER
        - userByEmail: ingest@example.com
          role: WRITER
        - groupByEmail: administrators@example.com
          role: OWNER
  ```

* **empty_tables**: LIST
  Create new tables, deleting them first if they already exist.

  For detailed information about table configuration parameters, see the [Google BigQuery Tables Documentation](https://cloud.google.com/bigquery/docs/reference/v2/tables#resource).

  Examples:

  ```
  empty_tables:
    - foo
    - other_dataset.bar
    - other_project:yet_another_dataset.baz
  ```

  ```
  empty_tables:
    - foo_table_${session_date_compact}
    - id: bar_table_${session_date_compact}
      project: other_project
      dataset: other_dataset
      friendly_name: Bar dataset ${session_date_compact}
      description: Bar dataset for ${session_date}
      expiration_time: 2016-11-01-T01:02:03Z
      schema:
        fields:
          - {name: foo, type: STRING}
          - {name: bar, type: INTEGER}
      labels:
        foo: bar
        quux: 17
      access:
        - domain: example.com
          role: READER
        - userByEmail: ingest@example.com
          role: WRITER
        - groupByEmail: administrators@example.com
          role: OWNER
  ```

* **delete_tables**: LIST
  Delete tables, if they exist.

  Examples:

  ```
  delete_tables:
    - foo
    - other_dataset.bar
    - other_project:yet_another_dataset.baz
  ```

  ```
  delete_tables:
    - foo_table_${last_session_date_compact}
    - bar_table_${last_session_date_compact}
  ```

