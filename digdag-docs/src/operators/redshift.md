# redshift>: Redshift operations

**redshift>** operator runs queries and/or DDLs on Redshift.

```
_export:
  redshift:
    host: my-redshift.1234abcd.us-east-1.redshift.amazonaws.com
    # port: 5439
    database: production_db
    user: app_user
    ssl: true
    schema: myschema
    # strict_transaction: false

+replace_deduplicated_master_table:
  redshift>: queries/dedup_master_table.sql
  create_table: dedup_master

+prepare_summary_table:
  redshift>: queries/create_summary_table_ddl.sql

+insert_to_summary_table:
  redshift>: queries/join_log_with_master.sql
  insert_into: summary_table

+select_members:
  redshift>: select_members.sql
  store_last_results: first

+send_email:
  for_each>:
    member: ${redshift.last_results}
  _do:
    mail>: body.txt
    subject: Hello, ${member.name}!
    to: [${member.email}]
```

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **aws.redshift.password**: NAME

  Optional user password to use when connecting to the Redshift database. If you want to use multiple credentials, use `password_override` option.

## Options

* **redshift>**: FILE.sql

  Path of the query template file. This file can contain `${...}` syntax to embed variables.

  Examples:

  ```
  redshift>: queries/complex_queries.sql
  ```

* **create_table**: NAME

  Table name to create from the results. This option deletes the table if it already exists.

  This option adds DROP TABLE IF EXISTS; CREATE TABLE AS before the statements written in the query template file. Also, CREATE TABLE statement can be written in the query template file itself without this command.

  Examples:

  ```
  create_table: dest_table
  ```

* **insert_into**: NAME

  Table name to append results into.

  This option adds INSERT INTO before the statements written in the query template file. Also, INSERT INTO statement can be written in the query template file itself without this command.

  Examples:

  ```
  insert_into: dest_table
  ```

* **download_file**: NAME

  Local CSV file name to be downloaded. The file includes the result of query.

  Examples:

  ```
  download_file: output.csv
  ```

* **store_last_results**: false | first | all

  Whether to store the query results to ``redshift.last_results`` parameter. *Default:* `false`.

  Setting ``first`` stores the first row to the parameter as an object (e.g. ``${redshift.last_results.count}``).

  Setting ``all`` stores all rows to the parameter as an array of objects (e.g. ``${redshift.last_results[0].name}``). If number of rows exceeds limit, task fails.

  Examples:

  ```
  store_last_results: first
  ```

  ```
  store_last_results: all
  ```

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
  If the query that created the status table completed 24 hours ago, this operator drop the table in the cleanup step.

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

* **connect_timeout**: NAME

  The timeout value used for socket connect operations. If connecting to the server takes longer than this value, the connection is broken. *Default*: `30s`(30 seconds).

  Examples:

  ```
  connect_timeout: 30s
  ```

* **socket_timeout**: NAME

  The timeout value used for socket read operations. If reading from the server takes longer than this value, the connection is closed. *Default*: `1800s`(1800 seconds).

  Examples:

  ```
  socket_timeout: 1800s
  ```

* **password_override**: NAME

  Secret key name that has a non-default database password as its value. This would be useful whey you want to use multiple database credentials. If it's set, Digdag looks up secrets with this value as a secret key name. If not, the default secret key `aws.redshift.password` is used.

  Examples (let's say you've already added a secret key value `aws.redshift.another_password=password1234`):

  ```
  password_override: another_password
  ```

* **status_table_cleanup**: TIME VALUES

  Specifies the period of time to clean up the status_table. When "strict_transaction: true" (default), the status_table will be created. status_table will be deleted when the status_table_cleanup period expires and the redshift operator is executed. *Default*: `24h`(24 hours).

  Examples:

  ```
  status_table_cleanup: 5s
  ```
