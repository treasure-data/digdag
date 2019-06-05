# td_table_export>: Treasure Data table export to S3

NOTE: We're limiting export capability to only us-east region S3 bucket. In general, please use Result Output to S3 feature using td operator. This workflow example is [here](https://github.com/treasure-data/workflow-examples/tree/master/td/s3).

**td_table_export>** operator exports data from Treasure Data to S3.

    +step1:
      td_table_export>:
      database: mydb
      table: mytable
      file_format: jsonl.gz
      from: 2016-01-01 00:00:00 +0800
      to:   2016-02-01 00:00:00 +0800
      s3_bucket: my_backup_backet
      s3_path_prefix: mydb/mytable

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **td.apikey**: API_KEY

  The Treasure Data API key to use when running Treasure Data table exports.

* **aws.s3.access_key_id**: ACCESS_KEY_ID

  The AWS Access Key ID to use when writing to S3.

  Examples:

  ```
  aws.s3.access_key_id: ABCDEFGHJKLMNOPQRSTU
  ```

* **aws.s3.secret_access_key**: SECRET_ACCESS_KEY

  The AWS Secret Access Key to use when writing to S3.

  Examples:

  ```
  aws.s3.secret_access_key: QUtJ/QUpJWTQ3UkhZTERNUExTUEEQUtJQUpJWTQ3
  ```

## Options

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

* **file_format**: TYPE

  Output file format. Available formats are `tsv.gz` (tab-separated values per line) and `jsonl.gz` (json record per line).

  `json.gz` and `line-json.gz` are available only for backward compatibility purpose.

  Examples:

  ```
  file_format: jsonl.gz
  ```

* **from**: yyyy-MM-dd HH:mm:ss[ Z]

  Export records from this time (inclusive). Actual time range is :command:`[from, to)`. Value should be a UNIX timestamp integer (seconds) or string in yyyy-MM-dd HH:mm:ss[ Z] format.

  Examples:

  ```
  from: 2016-01-01 00:00:00 +0800
  ```

* **to**: yyyy-MM-dd HH:mm:ss[ Z]

  Export records to this time (exclusive). Actual time range is :command:`[from, to)`. Value should be a UNIX timestamp integer (seconds) or string in yyyy-MM-dd HH:mm:ss[ Z] format.

  Examples:

  ```
  to: 2016-02-01 00:00:00 +0800
  ```

* **s3_bucket**: NAME

  S3 bucket name to export records to.

  Examples:

  ```
  s3_bucket: my_backup_backet
  ```

* **s3_path_prefix**: NAME

  S3 file name prefix.

  Examples:

  ```
  s3_path_prefix: mytable/mydb
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
