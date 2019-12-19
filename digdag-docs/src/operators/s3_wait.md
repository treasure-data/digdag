# s3_wait>: Wait for a file in Amazon S3

**s3_wait>** operator waits for file to appear in Amazon S3.

    +wait:
      s3_wait>: my-bucket/my-key

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **aws.s3.access_key_id, aws.access_key_id**

  The AWS Access Key ID to use when accessing S3.

* **aws.s3.secret_access_key, aws.secret_access_key**

  The AWS Secret Access Key to use when accessing S3.

* **aws.s3.region, aws.region**

  An optional explicit AWS Region in which to access S3.

* **aws.s3.endpoint**

  An optional explicit API endpoint to use when accessing S3. This overrides the `region` secret.

* **aws.s3.sse_c_key**

  An optional Customer-Provided Server-Side Encryption (SSE-C) key to use when accessing S3. Must be Base64 encoded.

* **aws.s3.sse_c_key_algorithm**

  An optional Customer-Provided Server-Side Encryption (SSE-C) key algorithm to use when accessing S3.

* **aws.s3.sse_c_key_md5**

  An optional MD5 digest of the Customer-Provided Server-Side Encryption (SSE-C) key to use when accessing S3. Must be Base64 encoded.

For more information about SSE-C, See the [AWS S3 Documentation](http://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html).

## Options

* **s3_wait>**: BUCKET/KEY

  Path to the file in Amazon S3 to wait for.

  Examples:

  ```
  s3_wait>: my-bucket/my-data.gz
  ```

  ```
  s3_wait>: my-bucket/file/in/a/directory
  ```

* **region**: REGION

  An optional explicit AWS Region in which to access S3. This may also be specified using the `aws.s3.region` secret.

* **endpoint**: ENDPOINT

  An optional explicit AWS Region in which to access S3. This may also be specified using the `aws.s3.endpoint` secret.
  *Note:* This will override the `region` parameter.

* **bucket**: BUCKET

  The S3 bucket where the file is located. Can be used together with the `key` parameter instead of putting the path on the operator line.

* **key**: KEY

  The S3 key of the file. Can be used together with the `bucket` parameter instead of putting the path on the operator line.

* **version_id**: VERSION_ID

  An optional object version to check for.

* **path_style_access**: true/false

  An optional flag to control whether to use path-style or virtual hosted-style access when accessing S3.
  *Note:* Enabling `path_style_access` also requires specifying a `region`.

* **timeout**: TIMEOUT

  Set timeout.

  Examples: wait 120 seconds

  ```
  timeout: 120s
  ```

* **continue_on_timeout**: true/false (default:false)

  If continue_on_timeout is set to true, the task will finish successfully on timeout.
  s3.last_object is empty in this case. Empty check is required in following tasks if access to s3.last_object.

  ```
  +task1:
    s3_wait>: bucket/object
    timeout: 60s
    continue_on_timeout: true
  +task2:
    if>: ${s3.last_object}
    _do:
      echo>: "No timeout"
  ```

## Output Parameters

* **s3.last_object**

  Information about the detected file.

        {
          "metadata": {
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Content-Length": 4711,
            "Content-Type": "application/octet-stream",
            "ETag": "5eb63bbbe01eeed093cb22bb8f5acdc3",
            "Last-Modified": 1474360744000,
            "Last-Ranges": "bytes"
          },
          "user_metadata": {
            "foo": "bar",
            "baz": "quux"
          }
        }

Note: The **s3_wait>** operator makes use of polling with *exponential backoff*. As such there might be some time interval between a file being created and the **s3_wait>** operator detecting it.

