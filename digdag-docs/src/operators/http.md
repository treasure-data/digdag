# http>: Making HTTP requests

**http>** operator can be used to make HTTP requests.

```
+fetch:
  http>: https://api.example.com/foobars
  store_content: true

+process:
  for_each>:
    foobar: ${http.last_content}
  _do:
    bq>: query.sql
```

```
+notify:
  http>: https://api.example.com/data/sessions/${session_uuid}
  method: POST
  content:
    status: RUNNING
    time: ${session_time}
```

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **http.authorization**: STRING

  A string that should be included in the HTTP request as the value of the `Authorization` header. This can be used to authenticate using e.g. Oauth bearer tokens.

* **http.user**: STRING

  A user that should be used to authenticate using *Basic Authentication*.

* **http.password**: STRING

  A password that should be used to authenticate using *Basic Authentication*.

* **http.uri**: URI

  The URI of the HTTP request. This can be used instead of putting the URI on the operator command line in case the URI contains sensitive information.

## Parameters

* **http>**: URI

  The URI of the HTTP request.

  Examples:

  ```
  http>: https://api.example.com/foobar
  ```

  ```
  http>: https://api.example.com/data/sessions/${session_uuid}
  ```

* **method**: STRING

  The method of the HTTP request. *Default:* `GET`.

  Examples:

  ```
  method: POST
  ```

  ```
  method: DELETE
  ```

* **content**: STRING | INTEGER | BOOLEAN | OBJECT | ARRAY
  The content of the HTTP request. *Default:* No content.

  Scalars (i.e. strings, integers, booleans, etc) will by default be sent as plain text. Objects and arrays will by default be JSON serialized. The `content_format` parameter can be used to control the content serialization format.

  ```
  content: 'hello world'
  ```

  ```
  content: '${session_time}'
  ```

  ```
  content:
    status: RUNNING
    time: ${session_time}
  ```

* **content_format**: text | json | form

  The serialization format of the content of the HTTP request. *Default:* Inferred from the `content` parameter value type. Objects and arrays use `json` by default. Other value types default to `text`.

  * **text**: Send raw content as `Content-Type: text/plain`. *Note:* This requires that the `content` parameter is _not_ array or an object.

  * **json**: Serialize the content as [JSON](http://json.org/) and send it as `Content-Type: application/json`. This format can handle any `content` parameter value type.

  * **form**: Encode content as an HTML form and send it as `Content-Type: application/x-www-form-urlencoded`. *Note:* This requires the `content` parameter value to be an object.

  ```
  content: 'hello world @ ${session_time}'
  content_format: text
  ```

  ```
  content:
    status: RUNNING
    time: ${session_time}
  content_format: json
  ```

  ```
  content:
    status: RUNNING
    time: ${session_time}
  content_format: form
  ```

* **content_type**: STRING

  Override the inferred `Content-Type` header.

  ```
  content: |
    <?xml version="1.0" encoding="UTF-8"?>
    <notification>
      <status>RUNNING</status>
      <time>${session_time}</time>
    </notification>
  content_format: text
  content_type: application/xml
  ```

* **store_content**: BOOLEAN

  Whether to store the content of the response. *Default:* `false`.

* **headers**: LIST OF KEY-VALUE PAIRS

  Additional custom headers to send with the HTTP request.

  ```
    headers:
      - Accept: application/json
      - X-Foo: bar
      - Baz: quux
  ```

* **retry**: BOOLEAN

  Whether to retry ephemeral errors. *Default:* `true` if the request method is `GET`, `HEAD`, `OPTIONS` or `TRACE`. Otherwise `false`.

  Client `4xx` errors (except for `408 Request Timeout` and `429 Too Many Requests`) will not be retried even if `retry` is set to `true`.

  *Note:* Enabling retries might cause the target endpoint to receive multiple duplicate HTTP requests. Thus retries should only be enabled if duplicated requests are tolerable. E.g. when the outcome of the HTTP request is *idempotent*.

* **timeout**: INTEGER

  The timeout value used for http operations. *Default*: `30`(30 seconds).
