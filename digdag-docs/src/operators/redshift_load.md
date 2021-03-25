# redshift_load>: Redshift load operations

**redshift_load>** operator runs COPY statement to load data from external storage on Redshift.

    _export:
      redshift:
        host: my-redshift.1234abcd.us-east-1.redshift.amazonaws.com
        # port: 5439
        database: production_db
        user: app_user
        ssl: true
        # strict_transaction: false

    +load_from_dynamodb_simple:
        redshift_load>:
        schema: myschema
        table: transactions
        from: dynamodb://transaction-table
        readratio: 123

    +load_from_s3_with_many_options:
        redshift_load>:
        schema: myschema
        table: access_logs
        from: s3://my-app-bucket/access_logs/today
        manifest: true
        encrypted: true
        region: us-east-1
        csv: "'"
        delimiter: "$"
        # json: s3://my-app-bucket/access_logs/jsonpathfile
        # avro: auto
        # fixedwidth: host:15,code:3,method:15
        gzip: true
        # bzip2: true
        # lzop: true
        acceptanydate: true
        acceptinvchars: "&"
        blanksasnull: true
        dateformat: yyyy-MM-dd
        emptyasnull: true
        encoding: UTF8
        escape: false
        explicit_ids: true
        fillrecord: true
        ignoreblanklines: true
        ignoreheader: 2
        null_as: nULl
        removequotes: false
        roundec: true
        timeformat: YYYY-MM-DD HH:MI:SS
        trimblanks: true
        truncatecolumns: true
        comprows: 12
        compupdate: ON
        maxerror: 34
        # noload: true
        statupdate: false
        role_session_name: federated_user
        session_duration: 1800
        # temp_credentials: false


## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **aws.redshift.password**: NAME

  Optional user password to use when connecting to the Redshift database.

* **aws.redshift_load.access_key_id, aws.redshift.access_key_id, aws.access_key_id**

  The AWS Access Key ID to use when accessing data source. This value is used to get temporary security credentials by default. See `temp_credentials` option for details.

* **aws.redshift_load.secret_access_key, aws.redshift.secret_access_key, aws.secret_access_key**

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

* **table**: NAME

  Table name in Redshift database to be loaded data

  Examples:

  ```
  table: access_logs
  ```

* **from**: URI

  Parameter mapped to `FROM` parameter of Redshift's `COPY` statement

  Examples:

  ```
  from: s3://my-app-bucket/access_logs/today
  ```

* **manifest**: BOOLEAN

  Parameter mapped to `MANIFEST` parameter of Redshift's `COPY` statement

  Examples:

  ```
  manifest: true
  ```

* **encrypted**: BOOLEAN

  Parameter mapped to `ENCRYPTED` parameter of Redshift's `COPY` statement

  Examples:

  ```
  encrypted: true
  ```

* **readratio**: NUMBER

  Parameter mapped to `READRATIO` parameter of Redshift's `COPY` statement

  Examples:

  ```
  readratio: 150
  ```

* **region**: NAME

  Parameter mapped to `REGION` parameter of Redshift's `COPY` statement

  Examples:

  ```
  region: us-east-1
  ```

* **csv**: CHARACTER

  Parameter mapped to `CSV` parameter of Redshift's `COPY` statement.
  If you want to just use default quote character of `CSV` parameter, set empty string like `csv: ''`

  Examples:

  ```
  csv: "'"
  ```

* **delimiter**: CHARACTER

  Parameter mapped to `DELIMITER` parameter of Redshift's `COPY` statement

  Examples:

  ```
  delimiter: "$"
  ```

* **json**: URI

  Parameter mapped to `JSON` parameter of Redshift's `COPY` statement

  Examples:

  ```
  json: auto
  ```

  Examples:

  ```
  json: s3://my-app-bucket/access_logs/jsonpathfile
  ```

* **avro**: URI

  Parameter mapped to `AVRO` parameter of Redshift's `COPY` statement

  Examples:

  ```
  avro: auto
  ```

  ```
  avro: s3://my-app-bucket/access_logs/jsonpathfile
  ```

* **fixedwidth**: CSV

  Parameter mapped to `FIXEDWIDTH` parameter of Redshift's `COPY` statement

  Examples:

  ```
  fixedwidth: host:15,code:3,method:15
  ```

* **gzip**: BOOLEAN

  Parameter mapped to `GZIP` parameter of Redshift's `COPY` statement

  Examples:

  ```
  gzip: true
  ```

* **bzip2**: BOOLEAN

  Parameter mapped to `BZIP2` parameter of Redshift's `COPY` statement

  Examples:

  ```
  bzip2: true
  ```

* **lzop**: BOOLEAN

  Parameter mapped to `LZOP` parameter of Redshift's `COPY` statement

  Examples:

  ```
  lzop: true
  ```

* **acceptanydate**: BOOLEAN

  Parameter mapped to `ACCEPTANYDATE` parameter of Redshift's `COPY` statement

  Examples:

  ```
  acceptanydate: true
  ```

* **acceptinvchars**: CHARACTER

  Parameter mapped to `ACCEPTINVCHARS` parameter of Redshift's `COPY` statement

  Examples:

  ```
  acceptinvchars: "&"
  ```

* **blanksasnull**: BOOLEAN

  Parameter mapped to `BLANKSASNULL` parameter of Redshift's `COPY` statement

  Examples:

  ```
  blanksasnull: true
  ```

* **dateformat**: STRING

  Parameter mapped to `DATEFORMAT` parameter of Redshift's `COPY` statement

  Examples:

  ```
  dateformat: yyyy-MM-dd
  ```

* **emptyasnull**: BOOLEAN

  Parameter mapped to `EMPTYASNULL` parameter of Redshift's `COPY` statement

  Examples:

  ```
  emptyasnull: true
  ```

* **encoding**: TYPE

  Parameter mapped to `ENCODING` parameter of Redshift's `COPY` statement

  Examples:

  ```
  encoding: UTF8
  ```

* **escape**: BOOLEAN

  Parameter mapped to `ESCAPE` parameter of Redshift's `COPY` statement

  Examples:

  ```
  escape: false
  ```

* **explicit_ids**: BOOLEAN

  Parameter mapped to `EXPLICIT_IDS` parameter of Redshift's `COPY` statement

  Examples:

  ```
  explicit_ids: true
  ```

* **fillrecord**: BOOLEAN

  Parameter mapped to `FILLRECORD` parameter of Redshift's `COPY` statement

  Examples:

  ```
  fillrecord: true
  ```

* **ignoreblanklines**: BOOLEAN

  Parameter mapped to `IGNOREBLANKLINES` parameter of Redshift's `COPY` statement

  Examples:

  ```
  ignoreblanklines: true
  ```

* **ignoreheader**: NUMBER

  Parameter mapped to `IGNOREHEADER` parameter of Redshift's `COPY` statement

  Examples:

  ```
  ignoreheader: 2
  ```

* **null_as**: STRING

  Parameter mapped to `NULL AS` parameter of Redshift's `COPY` statement

  Examples:

  ```
  null_as: nULl
  ```

* **removequotes**: BOOLEAN

  Parameter mapped to `REMOVEQUOTES` parameter of Redshift's `COPY` statement

  Examples:

  ```
  removequotes: false
  ```

* **roundec**: BOOLEAN

  Parameter mapped to `ROUNDEC` parameter of Redshift's `COPY` statement

  Examples:

  ```
  roundec: true
  ```

* **timeformat**: STRING

  Parameter mapped to `TIMEFORMAT` parameter of Redshift's `COPY` statement

  Examples:

  ```
  timeformat: YYYY-MM-DD HH:MI:SS
  ```

* **trimblanks**: BOOLEAN

  Parameter mapped to `TRIMBLANKS` parameter of Redshift's `COPY` statement

  Examples:

  ```
  trimblanks: true
  ```

* **truncatecolumns**: BOOLEAN

  Parameter mapped to `TRUNCATECOLUMNS` parameter of Redshift's `COPY` statement

  Examples:

  ```
  truncatecolumns: true
  ```

* **comprows**: NUMBER

  Parameter mapped to `COMPROWS` parameter of Redshift's `COPY` statement

  Examples:

  ```
  comprows: 12
  ```

* **compupdate**: TYPE

  Parameter mapped to `COMPUPDATE` parameter of Redshift's `COPY` statement

  Examples:

  ```
  compupdate: ON
  ```

* **maxerror**: NUMBER

  Parameter mapped to `MAXERROR` parameter of Redshift's `COPY` statement

  Examples:

  ```
  maxerror: 34
  ```

* **noload**: BOOLEAN

  Parameter mapped to `NOLOAD` parameter of Redshift's `COPY` statement

  Examples:

  ```
  noload: true
  ```

* **statupdate**: TYPE

  Parameter mapped to `STATUPDATE` parameter of Redshift's `COPY` statement

  Examples:

  ```
  statupdate: off
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
