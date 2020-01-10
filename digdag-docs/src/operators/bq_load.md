# bq_load>: Importing Data into Google BigQuery

**bq_load>** operator can be used to import data into Google BigQuery tables.

    _export:
      bq:
        dataset: my_dataset

    +ingest:
      bq_load>: gs://my_bucket/data.csv
      destination_table: my_data

    +process:
      bq>: queries/process.sql
      destination_table: my_result

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **gcp.credential**: CREDENTIAL

  See [gcp.credential](bq.html#secrets).


## Options

* **bq_load>**: URI | LIST

  A URI or list of URIs identifying files in GCS to import.

  Examples:

  ```
  bq_load>: gs://my_bucket/data.csv
  ```

  ```
  bq_load>:
    - gs://my_bucket/data1.csv.gz
    - gs://my_bucket/data2_*.csv.gz
  ```

* **dataset**: NAME

  The dataset that the destination table is located in or should be created in. Can also be specified directly in the table reference.

  Examples:

  ```
  dataset: my_dataset
  ```

  ```
  dataset: my_project:my_dataset
  ```

* **destination_table**: NAME

  The table to store the imported data in.

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

* **project**: NAME

  The project that the table is located in or should be created in. Can also be specified directly in the table reference or the dataset parameter.

* **source_format**: CSV | NEWLINE_DELIMITED_JSON | AVRO | DATASTORE_BACKUP

  The format of the files to be imported. *Default*: `CSV`.

  Examples:

  ```
  source_format: CSV
  ```

  ```
  source_format: NEWLINE_DELIMITED_JSON
  ```

  ```
  source_format: AVRO
  ```

  ```
  source_format: DATASTORE_BACKUP
  ```

* **field_delimiter**: CHARACTER

  The separator used between fields in CSV files to be imported. *Default*: `,`.

  Examples:

  ```
  field_delimiter: '\\t'
  ```

* **create_disposition**: CREATE_IF_NEEDED | CREATE_NEVER

  Specifies whether the destination table should be automatically created when performing the import.

  - `CREATE_IF_NEEDED`: *(default)* The destination table is created if it does not already exist.
  - `CREATE_NEVER`: The destination table must already exist, otherwise the import will fail.

  Examples:

  ```
  create_disposition: CREATE_IF_NEEDED
  ```

  ```
  create_disposition: CREATE_NEVER
  ```

* **write_disposition**: WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY

  Specifies whether to permit importing data to an already existing destination table.

  - `WRITE_TRUNCATE`: If the destination table already exists, any data in it will be overwritten.
  - `WRITE_APPEND`: If the destination table already exists, any data in it will be appended to.
  - `WRITE_EMPTY`: *(default)* The import fails if the destination table already exists and is not empty.

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

* **skip_leading_rows**: INTEGER

  The number of leading rows to skip in CSV files to import. *Default*: `0`.

  Examples:

  ```
  skip_leading_rows: 1
  ```

* **encoding**: UTF-8 | ISO-8859-1
  The character encoding of the data in the files to import. *Default*: `UTF-8`.

  Examples:

  ```
  encoding: ISO-8859-1
  ```

* **quote**: CHARACTER

  The character quote of the data in the files to import. *Default*: `'"'`.

  Examples:

  ```
  quote: ''
  ```

  ```
  quote: "'"
  ```

* **max_bad_records**: INTEGER

  The maximum number of bad records to ignore before failing the import. *Default*: `0`.

  Examples:

  ```
  max_bad_records: 100
  ```

* **allow_quoted_newlines**: BOOLEAN

  Whether to allow quoted data sections that contain newline characters in a CSV file. *Default*: `false`.

* **allow_jagged_rows**: BOOLEAN

  Whether to accept rows that are missing trailing optional columns in CSV files. *Default*: `false`.

* **ignore_unknown_values**: BOOLEAN

  Whether to ignore extra values in data that are not represented in the table schema. *Default*: `false`.

* **projection_fields**: LIST

  A list of names of Cloud Datastore entity properties to load. Requires `source_format: DATASTORE_BACKUP`.

* **autodetect**: BOOLEAN

  Whether to automatically infer options and schema for CSV and JSON sources. *Default*: `false`.

* **schema_update_options**: LIST

  A list of destination table schema updates that may be automatically performed when performing the import.

    schema_update_options:
      - ALLOW_FIELD_ADDITION
      - ALLOW_FIELD_RELAXATION

* **schema**: OBJECT | STRING

  A table schema. It can accept object, json or yml file path.

  Example:

  You can write schema within .dag file directly.

  ```yaml
  +step:
    bq_load>: gs://<bucket>/path/to_file
    ...
    schema:
      fields:
        - name: "name"
          type: "string"
        ...
  ```

  Or you can write it as external file.

  ```java
  {
    "fields": [
      {"name": "name", "type": "STRING"},
      ...
    ]
  }
  ```
  ```yaml
  fields:
    - name: "name"
      type: "string"
    ...
  ```

  And specify the file path. Supported formats are YAML and JSON. If an extension of the path is `.json` bq_load try parse as JSON, otherwise YAML.

  ```yaml
  +step:
    bq_load>: gs://<bucket>/path/to_file
    ...
    schema: path/to/schema.json
    # or
    # schema: path/to/schema.yml
  ```

## Output parameters

* **bq.last_job_id**

  The id of the BigQuery job that performed this import.

  Note: `bq.last_jobid` parameter is kept only for backward compatibility but you must not use it because it will be removed removed in a near future release.

