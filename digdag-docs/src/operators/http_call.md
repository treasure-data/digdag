# http_call>: Call workflow fetched by HTTP

**http_call>** operator makes a HTTP request, parse response body, and embeds it as a subtask.

This operator parses response body based on returned Content-Type header. Content-Type must be set and following values are supported:

* **application/json**: Parse the response as JSON.
* **application/x-yaml**: Use the returned body as-is.

## Options

* **http_call>**: URI

  The URI of the HTTP request.

  Examples:

  ```
  http_call>: https://api.example.com/foobar
  ```

Same parameters with **http>** operator are also supported except the parameters listed bellow. See also [http> operator document](../http.html).

* store_content

