# redshift_unload>: Redshift unload operations

**redshift_unload>** operator runs UNLOAD statement to export data to external storage on Redshift.

    _export:
      redshift:
        host: my-redshift.1234abcd.us-east-1.redshift.amazonaws.com
        # port: 5439
        database: production_db
        user: app_user
        ssl: true
        schema: myschema
        # strict_transaction: false

    +load_from_s3_with_many_options:
        redshift_unload>:
        query: select * from access_logs
        to: s3://my-app-bucket/access_logs/today
        manifest: true
        encrypted: true
        delimiter: "$"
        # fixedwidth: host:15,code:3,method:15
        gzip: true
        # bzip2: true
        null_as: nULl
        escape: false
        addquotes: true
        parallel: ON

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **aws.redshift.password**: NAME

  Optional user password to use when connecting to the Redshift database.

* **aws.redshift_unload.access_key_id, aws.redshift.access_key_id, aws.access_key_id**

  The AWS Access Key ID to use when accessing data source. This value is used to get temporary security credentials by default. See `temp_credentials` option for details.

* **aws.redshift_unload.secret_access_key, aws.redshift.secret_access_key, aws.secret_access_key**

  The AWS Secret Access Key to use when accessing data source. This value is used to get temporary security credentials by default. See `temp_credentials` option for details.

* **aws.redshift_load.role_arn, aws.redshift.role_arn, aws.role_arn**

  Optional Amazon resource names (ARNs) used to copy data to the Redshift. The role needs `AssumeRole` role to use this option. Requires `temp_credentials` to be true.
  If this option isn't specified, this operator tries to use a federated user


## Options

* **database**: NAME

  Database name.

  Examples:

  ```
  database: my_db
  ```

* **host**: NAME

  Hostname or IP address of the database.

  Examples:

  ```
  host: db.foobar.com
  ```

* **port**: NUMBER

  Port number to connect to the database. *Default*: `5439`.

  Examples:

  ```
  port: 2345
  ```

* **user**: NAME

  User to connect to the database

  Examples:

  ```
  user: app_user
  ```

* **ssl**: BOOLEAN

  Enable SSL to connect to the database. *Default*: `false`.

  Examples:

  ```
  ssl: true
  ```

* **schema**: NAME

  Default schema name. *Default*: `public`.

  Examples:

  ```
  schema: my_schema
  ```

* **strict_transaction**: BOOLEAN

  Whether this operator uses a strict transaction to prevent generating unexpected duplicated records just in case. *Default*: `true`.
  This operator creates and uses a status table in the database to make an operation idempotent. But if creating a table isn't allowed, this option should be false.

  Examples:

  ```
  strict_transaction: false
  ```

* **status_table_schema**: NAME

  Schema name of status table. *Default*: same as the value of `schema` option.

  Examples:

  ```
  status_table_schema: writable_schema
  ```

* **status_table**: NAME

  Table name prefix of status table. *Default*: `__digdag_status`.

  Examples:

  ```
  status_table: customized_status_table
  ```

* **query**: STRING

  SELECT query. The results of the query are unloaded.

  Examples:

  ```
  query: select * from access_logs
  ```

* **to**: URI

  Parameter mapped to `TO` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  to: s3://my-app-bucket/access_logs/today
  ```

* **manifest**: BOOLEAN

  Parameter mapped to `MANIFEST` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  manifest: true
  ```

* **encrypted**: BOOLEAN

  Parameter mapped to `ENCRYPTED` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  encrypted: true
  ```

* **allowoverwrite**: BOOLEAN

  Parameter mapped to `ALLOWOVERWRITE` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  allowoverwrite: true
  ```

* **delimiter**: CHARACTER

  Parameter mapped to `DELIMITER` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  delimiter: "$"
  ```

* **fixedwidth**: BOOLEAN

  Parameter mapped to `FIXEDWIDTH` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  fixedwidth: host:15,code:3,method:15
  ```

* **gzip**: BOOLEAN

  Parameter mapped to `GZIP` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  gzip: true
  ```

* **bzip2**: BOOLEAN

  Parameter mapped to `BZIP2` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  bzip2: true
  ```

* **null_as**: BOOLEAN

  Parameter mapped to `NULL_AS` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  null_as: nuLL
  ```

* **escape**: BOOLEAN

  Parameter mapped to `ESCAPE` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  escape: true
  ```

* **addquotes**: BOOLEAN

  Parameter mapped to `ADDQUOTES` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  addquotes: true
  ```

* **parallel**: TYPE

  Parameter mapped to `PARALLEL` parameter of Redshift's `UNLOAD` statement

  Examples:

  ```
  parallel: ON
  ```

* **temp_credentials**: BOOLEAN

  Whether this operator uses temporary security credentials. *Default*: `true`.
  This operator tries to use temporary security credentials as follows:
    - If `role_arn` is specified, it calls `AssumeRole` action
    - If not, it calls `GetFederationToken` action

  See details about `AssumeRole` and `GetFederationToken` in the documents of AWS Security Token Service.

  So either of `AssumeRole` or `GetFederationToken` action is called to use temporary security credentials by default for secure operation.
  But if this option is disabled, this operator uses credentials as-is set in the secrets insread of temporary security credentials.

  Examples:

  ```
  temp_credentials: false
  ```

* **session_duration INTEGER**

  Session duration of temporary security credentials. *Default*: `3 hour`.
  This option isn't used when disabling `temp_credentials`

  Examples:

  ```
  session_duration: 1800
  ```
