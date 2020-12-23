# bq_extract>: Exporting Data from Google BigQuery

**bq_extract>** operator can be used to export data from Google BigQuery tables.

    _export:
      bq:
        dataset: my_dataset

    +process:
      bq>: queries/analyze.sql
      destination_table: result

    +export:
      bq_extract>: result
      destination: gs://my_bucket/result.csv.gz
      compression: GZIP

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **gcp.credential**: CREDENTIAL

  See [gcp.credential](bq.html#secrets).

## Options

* **bq_extract>**: TABLE
  A reference to the table that should be exported.

  Examples:

  ```
  bq_extract>: my_table
  ```

  ```
  bq_extract>: my_dataset.my_table
  ```

  ```
  bq_extract>: my_project:my_dataset.my_table
  ```

* **destination**: URI | LIST
  A URI or list of URIs with the location of the destination export files. These must be Google Cloud Storage URIs.

  Examples:

  ```
  destination: gs://my_bucket/my_export.csv
  ```

  ```
  destination:
    - gs://my_bucket/my_export_1.csv
    - gs://my_bucket/my_export_2.csv
  ```

* **print_header**: BOOLEAN
  Whether to print out a header row in the results. *Default*: `true`.

* **field_delimiter**: CHARACTER
  A delimiter to use between fields in the output. *Default*: `,`.

  Examples:

  ```
  field_delimiter: "\t"
  ```

* **destination_format**: CSV | NEWLINE_DELIMITED_JSON | AVRO
  The format of the destination export file. *Default*: `CSV`.

  Examples:

  ```
  destination_format: CSV
  ```

  ```
  destination_format: NEWLINE_DELIMITED_JSON
  ```

  ```
  destination_format: AVRO
  ```

* **compression**: GZIP | NONE
  The compression to use for the export file. *Default*: `NONE`.

  Examples:

  ```
  compression: NONE
  ```

  ```
  compression: GZIP
  ```

## Output parameters

* **bq.last_job_id**

  The id of the BigQuery job that performed this export.

  Note: `bq.last_jobid` parameter is kept only for backward compatibility but you must not use it because it will be removed removed in a near future release.

