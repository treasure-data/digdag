# gcs_wait>: Wait for a file in Google Cloud Storage

**gcs_wait>** operator can be used to wait for file to appear in Google Cloud Storage.

    +wait:
      gcs_wait>: my_bucket/some/file

    +wait:
      gcs_wait>: gs://my_bucket/some/file

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **gcp.credential**: CREDENTIAL

  See [gcp.credential](bq.html#secrets).

## Options

* **gcs_wait>**: URI | BUCKET/OBJECT

  Google Cloud Storage URI or path of the file to wait for.

  Examples:

  ```
  gcs_wait>: my-bucket/my-directory/my-data.gz
  ```

  Examples:

  ```
  gcs_wait>: gs://my-bucket/my-directory/my-data.gz
  ```

* **bucket**: NAME

  The GCS bucket where the file is located. Can be used together with the `object` parameter instead of putting the path on the operator command line.

* **object**: PATH

  The GCS path of the file. Can be used together with the `bucket` parameter instead of putting the path on the operator command line.


## Output parameters

* **gcs_wait.last_object**

  Information about the detected file.

        {
            "metadata": {
                "bucket": "my_bucket",
                "contentType": "text/plain",
                "crc32c": "yV/Pdw==",
                "etag": "CKjJ6/H4988CEAE=",
                "generation": 1477466841081000,
                "id": "my_bucket/some/file",
                "kind": "storage#object",
                "md5Hash": "IT4zYwc3D23HpSGe3nZ85A==",
                "mediaLink": "https://www.googleapis.com/download/storage/v1/b/my_bucket/o/some%2Ffile?generation=1477466841081000&alt=media",
                "metageneration": 1,
                "name": "some/file",
                "selfLink": "https://www.googleapis.com/storage/v1/b/my_bucket/o/some%2Ffile",
                "size": 4711,
                "storageClass": "STANDARD",
                "timeCreated": {
                    "value": 1477466841070,
                    "dateOnly": false,
                    "timeZoneShift": 0
                },
                "updated": {
                    "value": 1477466841070,
                    "dateOnly": false,
                    "timeZoneShift": 0
                }
            }
        }

Note: The **gcs_wait>** operator makes use of polling with *exponential backoff*. As such there might be some time interval between a file being created and the **gcs_wait>** operator detecting it.
