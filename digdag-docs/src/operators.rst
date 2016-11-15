Operators
==================================

.. contents::
   :local:
   :depth: 2

call>: Calls another workflow
----------------------------------

**call>:** operator calls another workflow.

This operator embeds another workflow as a subtask.

.. code-block:: yaml

    # workflow1.dig
    +step1:
      call>: another_workflow.dig
    +step2:
      call>: common/shared_workflow.dig

.. code-block:: yaml

    # another_workflow.dig
    +another:
      sh>: ../scripts/my_script.sh

:command:`call>: FILE`
  Path to a workflow definition file. File name must end with ``.dig``.
  If called workflow is in a subdirectory, the workflow uses the subdirectory as the working directory. For example, a task has ``call>: common/called_workflow.dig``, using ``queries/data.sql`` file in the called workflow should be ``../queries/data.sql``.

  Example: another_workflow.dig

require>: Depends on another workflow
----------------------------------

**require>:** operator runs another workflow. Unlike ``call>`` operator, the workflow is skipped if the workflow for the session time is already done successfully before.

This operator submits a new session to digdag.

.. code-block:: yaml

    # workflow1.dig
    +step1:
      require>: another_workflow

.. code-block:: yaml

    # another_workflow.dig
    +step2:
      sh>: tasks/step2.sh

:command:`require>: NAME`
  Name of a workflow.

  Example: another_workflow

py>: Python scripts
----------------------------------

**py>:** operator runs a Python script using ``python`` command.

See `Python API documents <python_api.html>`_ for details including variable mappings to keyword arguments.

.. code-block:: yaml

    +step1:
      py>: my_step1_method
    +step2:
      py>: tasks.MyWorkflow.step2

:command:`py>: [PACKAGE.CLASS.]METHOD`
  Name of a method to run.

  * :command:`py>: tasks.MyWorkflow.my_task`


rb>: Ruby scripts
----------------------------------

**rb>:** operator runs a Ruby script using ``ruby`` command.

See `Ruby API documents <ruby_api.html>`_ for details including best practices how to configure the workflow using ``_export: require:``.

.. code-block:: yaml

    _export:
      rb:
        require: tasks/my_workflow

    +step1:
      rb>: my_step1_method
    +step2:
      rb>: Task::MyWorkflow.step2

:command:`rb>: [MODULE::CLASS.]METHOD`
  Name of a method to run.

  * :command:`rb>: Task::MyWorkflow.my_task`

:command:`require: FILE`
  Name of a file to require.

  * :command:`require: task/my_workflow`


sh>: Shell scripts
----------------------------------

**sh>:** operator runs a shell script.

Run a shell command (`/bin/sh`)

.. code-block:: yaml

    +step1:
      sh>: echo "hello world"


Run a shell script

.. code-block:: yaml

    +step1:
      sh>: tasks/step1.sh
    +step2:
      sh>: tasks/step2.sh

:command:`sh>: COMMAND [ARGS...]`
  Name of the command to run.

  * :command:`sh>: tasks/workflow.sh --task1`

The shell defaults to `/bin/sh`. If an alternate shell such as `zsh` is desired, use the `shell` option in the `_export` section.

.. code-block:: yaml

    _export:
      sh:
        shell: [/usr/bin/zsh]


loop>: Repeat tasks
----------------------------------

**loop>:** operator runs subtasks multiple times.

This operator exports ``${i}`` variable for the subtasks. Its value begins from 0. For example, if count is 3, a task runs with i=0, i=1, and i=2.

(This operator is EXPERIMENTAL. Parameters may change in a future release)

.. code-block:: yaml

    +repeat:
      loop>: 7
      _do:
        +step1:
          sh>: echo ${new Date((session_unixtime + i * 60 * 60 * 24) * 1000).toLocaleDateString()} is ${i} days later than $session_date
        +step2:
          sh>: echo ${
                new Date((session_unixtime + i * 60 * 60) * 1000).toLocaleDateString()
                + " "
                + new Date((session_unixtime + i * 60 * 60) * 1000).toLocaleTimeString()
            } is ${i} hours later than ${session_local_time}

:command:`loop>: COUNT`
  Number of times to run the tasks.

  * :command:`loop>: 7`

:command:`_parallel: BOOLEAN`
  Runs the repeating tasks in parallel.

  * :command:`_parallel: true`

:command:`_do: TASKS`
  Tasks to run.


for_each>: Repeat tasks
----------------------------------

**for_each>:** operator runs subtasks multiple times using sets of variables.

(This operator is EXPERIMENTAL. Parameters may change in a future release)

.. code-block:: yaml

    +repeat:
      for_each>:
        fruit: [apple, orange]
        verb: [eat, throw]
      _do:
        sh>: echo ${verb} ${fruit}
        # this will generate 4 tasks:
        #  +for-fruit=apple&verb=eat:
        #    sh>: echo eat apple
        #  +for-fruit=apple&verb=throw:
        #    sh>: echo throw apple
        #  +for-fruit=orange&verb=eat:
        #    sh>: echo eat orange
        #  +for-fruit=orange&verb=throw:
        #    sh>: echo throw orange

:command:`for_each>: VARIABLES`
  Variables used for the loop in ``key: [value, value, ...]`` syntax. Variables can be an object or JSON string.

  * :command:`for_each>: {i: [1, 2, 3]}`
  * or :command:`for_each>: {i: '[1, 2, 3]'}`

:command:`_parallel: BOOLEAN`
  Runs the repeating tasks in parallel.

  * :command:`_parallel: true`

:command:`_do: TASKS`
  Tasks to run.


if>: Conditional execution
----------------------------------

**if>:** operator runs subtasks if ``true`` is given.

(This operator is EXPERIMENTAL. Parameters may change in a future release)

.. code-block:: yaml

    +run_if_param_is_true:
      if>: ${param}
      _do:
        sh>: echo ${param} == true

:command:`if>: BOOLEAN`
  ``true`` or ``false``.

:command:`_do: TASKS`
  Tasks to run if ``true`` is given.

fail>: make the workflow failed
----------------------------------

**fail>:** always fails and makes the workflow failed.

(This operator is EXPERIMENTAL. Parameters may change in a future release)

This operator is useful used with **if>** operator to validate resuls of a previous task with ``_check`` directive so that a workflow fails when the validation doesn't pass.

.. code-block:: yaml

    +fail_if_too_few:
      if>: ${count < 10}
      _do:
        fail>: count is less than 10!

:command:`fail>: STRING`
  Message so that ``_error`` task can refer the message using ``${error.message}`` syntax.


td>: Treasure Data queries
----------------------------------

**td>:** operator runs a Hive or Presto query on Treasure Data.

TODO: add more description here

.. code-block:: yaml

    _export:
      td:
        database: www_access

    +step1:
      td>: queries/step1.sql
    +step2:
      td>: queries/step2.sql
      create_table: mytable_${session_date_compact}
    +step3:
      td>: queries/step2.sql
      insert_into: mytable

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when running Treasure Data queries.

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td>: FILE.sql`
  Path to a query template file. This file can contain ``${...}`` syntax to embed variables.

  * :command:`td>: queries/step1.sql`

:command:`create_table: NAME`
  Name of a table to create from the results. This option deletes the table if it already exists.

  This option adds DROP TABLE IF EXISTS; CREATE TABLE AS (Presto) or INSERT OVERWRITE (Hive) commands before the SELECT statement. If the query includes a ``-- DIGDAG_INSERT_LINE`` line, the commands are inserted there.

  * :command:`create_table: my_table`

:command:`insert_into: NAME`
  Name of a table to append results into. The table is created if it does not already exist.

  This option adds INSERT INTO (Presto) or INSERT INTO TABLE (Hive) command at the beginning of SELECT statement. If the query includes ``-- DIGDAG_INSERT_LINE`` line, the command is inserted to the line.

  * :command:`insert_into: my_table`

:command:`download_file: NAME`
  Saves query result as a local CSV file.

  * :command:`download_file: output.csv`

:command:`store_last_results: BOOLEAN`
  Stores the first 1 row of the query results to ``${td.last_results}`` variable (default: false).
  td.last_results is a map of column name and a value. To access to a single value, you can use ``${td.last_results.my_count}`` syntax.

  * :command:`store_last_results: true`

:command:`preview: BOOLEAN`
  Tries to show some query results to confirm the results of a query.

  * :command:`preview: true`

:command:`result_url: NAME`
  Output the query results to the URL:

  * :command:`result_url: tableau://username:password@my.tableauserver.com/?mode=replace`

:command:`database: NAME`
  Name of a database.

  * :command:`database: my_db`

:command:`endpoint: ADDRESS`
  API endpoint (default: api.treasuredata.com).

:command:`use_ssl: BOOLEAN`
  Enable SSL (https) to access to the endpoint (default: true).

:command:`engine: presto`
  Query engine (``presto`` or ``hive``).

  * :command:`engine: hive`
  * :command:`engine: presto`

:command:`priority: 0`
  Set Priority (From ``-2`` (VERY LOW) to ``2`` (VERY HIGH) , default: 0 (NORMAL)).


Output parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td.last_job_id`
  The job id this task executed.

  * :command:`52036074`

:command:`td.last_results`
  The first 1 row of the query results as a map. This is available only when ``store_last_results: true`` is set.

  * :command:`{"path":"/index.html","count":1}`

td_run>: Treasure Data saved queries
----------------------------------

**td_run>:** operator runs a query saved on Treasure Data.

TODO: add more description here

.. code-block:: yaml

    _export:
      td:
        database: www_access

    +step1:
      td_run>: myquery1
    +step2:
      td_run>: myquery2
      session_time: 2016-01-01T01:01:01+0000

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when running Treasure Data queries.

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td_run>: NAME`
  Name of a saved query.

  * :command:`td_run>: my_query`

:command:`download_file: NAME`
  Saves query result as a local CSV file.

  * :command:`download_file: output.csv`

:command:`store_last_results: BOOLEAN`
  Stores the first 1 row of the query results to ``${td.last_results}`` variable (default: false).
  td.last_results is a map of column name and a value. To access to a single value, you can use ``${td.last_results.my_count}`` syntax.

  * :command:`store_last_results: true`

:command:`preview: BOOLEAN`
  Tries to show some query results to confirm the results of a query.

  * :command:`preview: true`

:command:`endpoint: ADDRESS`
  API endpoint (default: api.treasuredata.com).

:command:`use_ssl: BOOLEAN`
  Enable SSL (https) to access to the endpoint (default: true).


Output parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td.last_job_id`
  The job id this task executed.

  * :command:`52036074`

:command:`td.last_results`
  The first 1 row of the query results as a map. This is available only when ``store_last_results: true`` is set.

  * :command:`{"path":"/index.html","count":1}`


td_load>: Treasure Data bulk loading
----------------------------------

**td_load>:** operator loads data from storages, databases, or services.

TODO: add more description here

.. code-block:: yaml

    +step1:
      td_load>: config/guessed.dig
      database: prod
      table: raw

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when submitting Treasure Data bulk load jobs.

Parameters
~~~~~~~~~~

:command:`td_load>: FILE.yml`
  Path to a YAML template file. This configuration needs to be guessed using td command.

  * :command:`td_load>: imports/load.yml`

:command:`database: NAME`
  Name of the database load data to.

  * :command:`database: my_database`

:command:`table: NAME`
  Name of the table load data to.

  * :command:`table: my_table`

:command:`endpoint: ADDRESS`
  API endpoint (default: api.treasuredata.com).

:command:`use_ssl: BOOLEAN`
  Enable SSL (https) to access to the endpoint (default: true).


Output parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td.last_job_id`
  The job id this task executed.

  * :command:`52036074`


td_ddl>: Treasure Data operations
----------------------------------

**td_ddl>** operator runs an operational task on Treasure Data.

TODO: add more description here

.. code-block:: yaml

    _export:
      td:
        database: www_access

    +step1:
      td_ddl>:
      create_tables: ["my_table_${session_date_compact}"]
    +step2:
      td_ddl>:
      drop_tables: ["my_table_${session_date_compact}"]
    +step2:
      td_ddl>:
      empty_tables: ["my_table_${session_date_compact}"]

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when performing Treasure Data operations.

Parameters
~~~~~~~~~~

:command:`create_tables: [ARRAY OF NAMES]`
  Create new tables if not exists.

  * :command:`create_tables: [my_table1, my_table2]`

:command:`empty_tables: [ARRAY OF NAME]`
  Create new tables (drop it first if it exists).

  * :command:`empty_tables: [my_table1, my_table2]`

:command:`drop_tables: [ARRAY OF NAMES]`
  Drop tables if exists.

  * :command:`drop_tables: [my_table1, my_table2]`

:command:`create_databases: [ARRAY OF NAMES]`
  Create new databases if not exists.

  * :command:`create_databases: [my_database1, my_database2]`

:command:`empty_databases: [ARRAY OF NAME]`
  Create new databases (drop it first if it exists).

  * :command:`empty_databases: [my_database1, my_database2]`

:command:`drop_databases: [ARRAY OF NAMES]`
  Drop databases if exists.

  * :command:`drop_databases: [my_database1, my_database2]`

:command:`endpoint: ADDRESS`
  API endpoint (default: api.treasuredata.com).

:command:`use_ssl: BOOLEAN`
  Enable SSL (https) to access to the endpoint (default: true).


td_table_export>: Treasure Data table export to S3
----------------------------------

**td_table_export>:** operator loads data from storages, databases, or services.

TODO: add more description here

.. code-block:: yaml

    +step1:
      td_table_export>:
      database: mydb
      table: mytable
      file_format: jsonl.gz
      from: 2016-01-01 00:00:00 +0800
      to:   2016-02-01 00:00:00 +0800
      s3_bucket: my_backup_backet
      s3_path_prefix: mydb/mytable

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when running Treasure Data table exports.

:command:`aws.s3.access-key-id: ACCESS_KEY_ID`
  The AWS Access Key ID to use when writing to S3.

  * :command:`aws.s3.access-key-id: ABCDEFGHJKLMNOPQRSTU`

:command:`aws.s3.secret-access-key: SECRET_ACCESS_KEY`
  The AWS Secret Access Key to use when writing to S3.

  * :command:`aws.s3.secret-access-key: QUtJ/QUpJWTQ3UkhZTERNUExTUEEQUtJQUpJWTQ3`


Parameters
~~~~~~~~~~

:command:`database: NAME`
  Name of the database.

  * :command:`database: my_database`

:command:`table: NAME`
  Name of the table to export.

  * :command:`table: my_table`

:command:`file_format: TYPE`
  Output file format. Available formats are ``tsv.gz``, ``jsonl.gz``, ``json.gz``, ``json-line.gz``.

  * :command:`file_format: jsonl.gz`

:command:`from: yyyy-MM-dd HH:mm:ss[ Z]`
  Export records from this time (inclusive). Actual time range is :command:`[from, to)`. Value should be a UNIX timestamp integer (seconds) or string in yyyy-MM-dd HH:mm:ss[ Z] format.

  * :command:`from: 2016-01-01 00:00:00 +0800`

:command:`to: yyyy-MM-dd HH:mm:ss[ Z]`
  Export records to this time (exclusive). Actual time range is :command:`[from, to)`. Value should be a UNIX timestamp integer (seconds) or string in yyyy-MM-dd HH:mm:ss[ Z] format.

  * :command:`to: 2016-02-01 00:00:00 +0800`

:command:`s3_bucket: NAME`
  S3 bucket name to export records to.

  * :command:`s3_bucket: my_backup_backet`

:command:`s3_path_prefix: NAME`
  S3 file name prefix.

  * :command:`s3_path_prefix: mytable/mydb`

:command:`endpoint: ADDRESS`
  API endpoint (default: api.treasuredata.com).

:command:`use_ssl: BOOLEAN`
  Enable SSL (https) to access to the endpoint (default: true).


Output parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td.last_job_id`
  The job id this task executed.

  * :command:`52036074`


pg>: PostgreSQL operations
----------------------------------

**pg>** operator runs queries and/or DDLs on PostgreSQL

.. code-block:: yaml


    _export:
      pg:
        host: 192.0.2.1
        port: 5430
        database: production_db
        user: app_user
        ssl: true

    +replace_deduplicated_master_table:
      pg>: queries/dedup_master_table.sql
      create_table: dedup_master

    +prepare_summary_table:
      pg>: queries/create_summary_table_ddl.sql

    +insert_to_summary_table:
      pg>: queries/join_log_with_master.sql
      insert_into: summary_table


Secrets
~~~~~~~

:command:`pg.password: NAME`
  Optional user password to use when connecting to the postgres database (default: empty)

Parameters
~~~~~~~~~~

:command:`pg>: FILE.sql`
  Path of the query template file. This file can contain ``${...}`` syntax to embed variables.

  * :command:`pg>: queries/complex_queries.sql`

:command:`create_table: NAME`
  Table name to create from the results. This option deletes the table if it already exists.

  This option adds DROP TABLE IF EXISTS; CREATE TABLE AS before the statements written in the query template file. Also, CREATE TABLE statement can be written in the query template file itself without this command.

  * :command:`create_table: dest_table`

:command:`insert_into: NAME`
  Table name to append results into.

  This option adds INSERT INTO before the statements written in the query template file. Also, INSERT INTO statement can be written in the query template file itself without this command.

  * :command:`insert_into: dest_table`

:command:`download_file: NAME`
  Local CSV file name to be downloaded. The file includes the result of query.

  * :command:`download_file: output.csv`

:command:`database: NAME`
  Database name.

  * :command:`database: my_db`

:command:`host: NAME`
  Hostname or IP address of the database.

  * :command:`host: db.foobar.com`

:command:`port: NUMBER`
  Port number to connect to the database (default: 5432).

  * :command:`port: 2345`

:command:`user: NAME`
  User to connect to the database

  * :command:`user: app_user`

:command:`ssl: BOOLEAN`
  Enable SSL to connect to the database (default: false).

  * :command:`ssl: true`

:command:`schema: NAME`
  Default schema name (default: public)

  * :command:`schema: my_schema`

TODO: Add some other commands


mail>: Sending email
----------------------------------

**mail>:** operator sends an email.

To use Gmail SMTP server, you need to do either of:

  a) Generate a new app password at `App passwords <https://security.google.com/settings/security/apppasswords>`_. This needs to enable 2-Step Verification first.

  b) Enable access for less secure apps at `Less secure apps <https://www.google.com/settings/security/lesssecureapps>`_. This works even if 2-Step Verification is not enabled.

.. code-block:: yaml

    _export:
      mail:
        from: "you@gmail.com"

    +step1:
      mail>: body.txt
      subject: workflow started
      to: [me@example.com]

    +step2:
      mail>:
        data: this is email body embedded in a .dig file
      subject: workflow started
      to: [me@example.com]

    +step3:
      sh>: this_task_might_fail.sh
      _error:
        mail>: body.txt
        subject: this workflow failed
        to: [me@example.com]

Secrets
~~~~~~~

:command:`mail.host: HOST`
  SMTP host name.

  * :command:`mail.host: smtp.gmail.com`

:command:`mail.port: PORT`
  SMTP port number.

  * :command:`mail.port: 587`

:command:`mail.username: NAME`
  SMTP login username.

  * :command:`mail.username: me`

:command:`mail.password: PASSWORD`
  SMTP login password.

  * :command:`mail.password: MyPaSsWoRd`

:command:`mail.tls: BOOLEAN`
  Enables TLS handshake.

  * :command:`mail.tls: true`

:command:`mail.ssl: BOOLEAN`
  Enables legacy SSL encryption.

  * :command:`mail.ssl: false`

Parameters
~~~~~~~~~~

:command:`mail>: FILE`
  Path to a mail body template file. This file can contain ``${...}`` syntax to embed variables.
  Alternatively, you can set ``{data: TEXT}`` to embed body text in the .dig file.

  * :command:`mail>: mail_body.txt`
  * or :command:`mail>: {body: Hello, this is from Digdag}`

:command:`subject: SUBJECT`
  Subject of the email.

  * :command:`subject: Mail From Digdag`

:command:`to: [ADDR1, ADDR2, ...]`
  To addresses.

  * :command:`to: [analyst@examile.com]`

:command:`from: ADDR`
  From address.

  * :command:`from: admin@example.com`

:command:`host: NAME`
  SMTP host name.

  * :command:`host: smtp.gmail.com`

:command:`port: NAME`
  SMTP port number.

  * :command:`port: 587`

:command:`username: NAME`
  SMTP login username.

  * :command:`username: me`

:command:`tls: BOOLEAN`
  Enables TLS handshake.

  * :command:`tls: true`

:command:`ssl: BOOLEAN`
  Enables legacy SSL encryption.

  * :command:`ssl: false`

:command:`html: BOOLEAN`
  Uses HTML mail (default: false).

  * :command:`html: true`

:command:`debug: BOOLEAN`
  Shows debug logs (default: false).

  * :command:`debug: false`

:command:`attach_files: ARRAY`
  Attach files. Each element is an object of:

  * :command:`path: FILE`: Path to a file to attach.

  * :command:`content_type`: Content-Type of this file. Default is application/octet-stream.

  * :command:`filename`: Name of this file. Default is base name of the path.

  Example:

  .. code-block:: yaml

      attach_files:
        - path: data.csv
        - path: output.dat
          filename: workflow_result_data.csv
        - path: images/image1.png
          content_type: image/png

embulk>: Embulk data transfer
----------------------------------

**embulk>:** operator runs `Embulk <http://www.embulk.org>`_ to transfer data across storages including local files.

.. code-block:: yaml

    +load:
      embulk>: data/load.yml

:command:`embulk>: FILE.yml`
  Path to a configuration template file.

  * :command:`embulk>: embulk/mysql_to_csv.yml`


s3_wait>: Wait for a file in Amazon S3
--------------------------------------

The **s3_wait>:** operator waits for file to appear in Amazon S3.

.. code-block:: yaml

    +wait:
      s3_wait>: my-bucket/my-key

Secrets
~~~~~~~

:command:`aws.s3.access-key-id, aws.access-key-id`
  The AWS Access Key ID to use when accessing S3.

:command:`aws.s3.secret-access-key, aws.secret-access-key`
  The AWS Secret Access Key to use when accessing S3.

:command:`aws.s3.region, aws.region`
  An optional explicit AWS Region in which to access S3.

:command:`aws.s3.endpoint`
  An optional explicit API endpoint to use when accessing S3. This overrides the `region` secret.

:command:`aws.s3.sse-c-key`
  An optional Customer-Provided Server-Side Encryption (SSE-C) key to use when accessing S3. Must be Base64 encoded.

:command:`aws.s3.sse-c-key-algorithm`
  An optional Customer-Provided Server-Side Encryption (SSE-C) key algorithm to use when accessing S3.

:command:`aws.s3.sse-c-key-md5`
  An optional MD5 digest of the Customer-Provided Server-Side Encryption (SSE-C) key to use when accessing S3. Must be Base64 encoded.

For more information about SSE-C, See the `AWS S3 Documentation <http://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html>`_.

Parameters
~~~~~~~~~~

:command:`s3_wait>: BUCKET/KEY`
  Path to the file in Amazon S3 to wait for.

  * :command:`s3_wait>: my-bucket/my-data.gz`

  * :command:`s3_wait>: my-bucket/file/in/a/directory`

:command:`region: REGION`
  An optional explicit AWS Region in which to access S3. This may also be specified using the `aws.s3.region` secret.

:command:`endpoint: ENDPOINT`
  An optional explicit AWS Region in which to access S3. This may also be specified using the `aws.s3.endpoint` secret.
  *Note:* This will override the `region` parameter.

:command:`bucket: BUCKET`
  The S3 bucket where the file is located. Can be used together with the `key` parameter instead of putting the path on the operator line.

:command:`key: KEY`
  The S3 key of the file. Can be used together with the `bucket` parameter instead of putting the path on the operator line.

:command:`version_id: VERSION_ID`
  An optional object version to check for.

:command:`path_style_access: true/false`
  An optional flag to control whether to use path-style or virtual hosted-style access when accessing S3.
  *Note:* Enabling `path_style_access` also requires specifying a `region`.

Output Parameters
~~~~~~~~~~~~~~~~~

:command:`s3.last_object`
  Information about the detected file.

    .. code-block:: yaml

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

.. note:: The **s3_wait>:** operator makes use of polling with *exponential backoff*. As such there might be some time interval between a file being created and the **s3_wait>:** operator detecting it.

bq>: Running Google BigQuery queries
------------------------------------

The **bq>:** operator can be used to run a query on Google BigQuery.


.. code-block:: yaml

    _export:
      bq:
        dataset: my_dataset

    +step1:
      bq>: queries/step1.sql
    +step2:
      bq>: queries/step2.sql
      destination_table: result_table
    +step3:
      bq>: queries/step3.sql
      destination_table: other_project:other_dataset.other_table


.. note:: The **bq>:** operator uses `standard SQL <https://cloud.google.com/bigquery/sql-reference/index>`_ by default, whereas the default in the BigQuery console is `legacy SQL <https://cloud.google.com/bigquery/query-reference>`_. To run *legacy* SQL queries, please set ``use_legacy_sql: true``. For more information about *standard* SQL on BigQuery, see `Migrating from legacy SQL <https://cloud.google.com/bigquery/sql-reference/migrating-from-legacy-sql>`_.

Secrets
~~~~~~~

.. _gcp_credential:

:command:`gcp.credential: CREDENTIAL`
  The `Google Cloud Platform account <https://cloud.google.com/docs/authentication#user_accounts_and_service_accounts>`_ credential private key to use, in JSON format.

  For information on how to generate a service account key, see the `Google Cloud Platform Documentation <https://cloud.google.com/storage/docs/authentication#generating-a-private-key>`_.

  Upload the private key JSON file to the digdag server using the ``secrets`` client command:

  .. code-block:: none

    digdag secrets --project my_project --set gcp.credential=@my-svc-account-b4df00d.json

Parameters
~~~~~~~~~~

:command:`bq>: query.sql`
  Path to a query template file. This file can contain ``${...}`` syntax to embed variables.

  * :command:`bq>: queries/step1.sql`

:command:`dataset: NAME`
  Specifies the default dataset to use in the query and in the ``destination_table`` parameter.

  * :command:`dataset: my_dataset`
  * :command:`dataset: other_project:other_dataset`

:command:`destination_table: NAME`
  Specifies a table to store the query results in.

  * :command:`destination_table: my_result_table`
  * :command:`destination_table: some_dataset.some_table`
  * :command:`destination_table: some_project:some_dataset.some_table`

:command:`create_disposition: CREATE_IF_NEEDED | CREATE_NEVER`
  Specifies whether the destination table should be automatically created when executing the query.

  - ``CREATE_IF_NEEDED``: *(default)* The destination table is created if it does not already exist.
  - ``CREATE_NEVER``: The destination table must already exist, otherwise the query will fail.

  Examples:

  * :command:`create_disposition: CREATE_IF_NEEDED`
  * :command:`create_disposition: CREATE_NEVER`

:command:`write_disposition: WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY`
  Specifies whether to permit writing of data to an already existing destination table.

  - ``WRITE_TRUNCATE``: If the destination table already exists, any data in it will be overwritten.
  - ``WRITE_APPEND``: If the destination table already exists, any data in it will be appended to.
  - ``WRITE_EMPTY``: *(default)* The query fails if the destination table already exists and is not empty.

  Examples:

  * :command:`write_disposition: WRITE_TRUNCATE`
  * :command:`write_disposition: WRITE_APPEND`
  * :command:`write_disposition: WRITE_EMPTY`

:command:`priority: INTERACTIVE | BATCH`
  Specifies the priority to use for this query. *Default*: ``INTERACTIVE``.

:command:`use_query_cache: BOOLEAN`
  Whether to use BigQuery query result caching. *Default*: ``true``.

:command:`allow_large_results: BOOLEAN`
  Whether to allow arbitrarily large result tables. Requires ``destination_table`` to be set and ``use_legacy_sql`` to be true.

:command:`flatten_results: BOOLEAN`
  Whether to flatten nested and repeated fields in the query results. *Default*: ``true``. Requires ``use_legacy_sql`` to be true.

:command:`use_legacy_sql: BOOLEAN`
  Whether to use legacy BigQuery SQL. *Default*: ``false``.

:command:`maximum_billing_tier: INTEGER`
  Limit the billing tier for this query. *Default*: The project default.

:command:`table_definitions: OBJECT`
  Describes external data sources that are accessed in the query. For more information see `BigQuery documentation <https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.tableDefinitions>`_.

:command:`user_defined_function_resources: LIST`
  Describes user-defined function resources used in the query. For more information see `BigQuery documentation <https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.userDefinedFunctionResources>`_.


Output parameters
~~~~~~~~~~~~~~~~~

:command:`bq.last_job_id`
  The id of the BigQuery job that executed this query.


bq_ddl>: Managing Google BigQuery Datasets and Tables
-----------------------------------------------------

The **bq_ddl>:** operator can be used to create, delete and clear Google BigQuery Datasets and Tables.


.. code-block:: yaml

    _export:
      bq:
        dataset: my_dataset

    +prepare:
      bq_ddl>:
        create_datasets:
          - my_dataset_${session_date_compact}
        empty_datasets:
          - my_dataset_${session_date_compact}
        delete_datasets:
          - my_dataset_${last_session_date_compact}
        create_tables:
          - my_table_${session_date_compact}
        empty_tables:
          - my_table_${session_date_compact}
        delete_tables:
          - my_table_${last_session_date_compact}


Secrets
~~~~~~~

:command:`gcp.credential: CREDENTIAL`
  See gcp_credential_.

Parameters
~~~~~~~~~~

:command:`create_datasets: LIST`
  Create new datasets.

  For detailed information about dataset configuration parameters, see the `Google BigQuery Datasets Documentation <https://cloud.google.com/bigquery/docs/reference/v2/datasets#resource>`_.

  Examples:

  .. code-block:: yaml

    create_datasets:
      - foo
      - other_project:bar

  .. code-block:: yaml

    create_datasets:
      - foo_dataset_${session_date_compact}
      - id: bar_dataset_${session_date_compact}
        project: other_project
        friendly_name: Bar dataset ${session_date_compact}
        description: Bar dataset for ${session_date}
        default_table_expiration: 7d
        location: EU
        labels:
          foo: bar
          quux: 17
        access:
          - domain: example.com
            role: READER
          - userByEmail: ingest@example.com
            role: WRITER
          - groupByEmail: administrators@example.com
            role: OWNER

:command:`empty_datasets: LIST`
  Create new datasets, deleting them first if they already exist. Any tables in the datasets will also be deleted.

  For detailed information about dataset configuration parameters, see the `Google BigQuery Datasets Documentation <https://cloud.google.com/bigquery/docs/reference/v2/datasets#resource>`_.

  Examples:

  .. code-block:: yaml

    empty_datasets:
      - foo
      - other_project:bar

  .. code-block:: yaml

    empty_datasets:
      - foo_dataset_${session_date_compact}
      - id: bar_dataset_${session_date_compact}
        project: other_project
        friendly_name: Bar dataset ${session_date_compact}
        description: Bar dataset for ${session_date}
        default_table_expiration: 7d
        location: EU
        labels:
          foo: bar
          quux: 17
        access:
          - domain: example.com
            role: READER
          - userByEmail: ingest@example.com
            role: WRITER
          - groupByEmail: administrators@example.com
            role: OWNER

:command:`delete_datasets: LIST`
  Delete datasets, if they exist.

  Examples:

  .. code-block:: yaml

    delete_datasets:
      - foo
      - other_project:bar

  .. code-block:: yaml

    delete_datasets:
      - foo_dataset_${last_session_date_compact}
      - other_project:bar_dataset_${last_session_date_compact}

:command:`create_tables: LIST`
  Create new tables.

  For detailed information about table configuration parameters, see the `Google BigQuery Tables Documentation <https://cloud.google.com/bigquery/docs/reference/v2/tables#resource>`_.

  Examples:

  .. code-block:: yaml

    create_tables:
      - foo
      - other_dataset.bar
      - other_project:yet_another_dataset.baz

  .. code-block:: yaml

    create_tables:
      - foo_dataset_${session_date_compact}
      - id: bar_dataset_${session_date_compact}
        project: other_project
        dataset: other_dataset
        friendly_name: Bar dataset ${session_date_compact}
        description: Bar dataset for ${session_date}
        expiration_time: 2016-11-01-T01:02:03Z
        schema:
          fields:
            - {name: foo, type: STRING}
            - {name: bar, type: INTEGER}
        labels:
          foo: bar
          quux: 17
        access:
          - domain: example.com
            role: READER
          - userByEmail: ingest@example.com
            role: WRITER
          - groupByEmail: administrators@example.com
            role: OWNER

:command:`empty_tables: LIST`
  Create new tables, deleting them first if they already exist.

  For detailed information about table configuration parameters, see the `Google BigQuery Tables Documentation <https://cloud.google.com/bigquery/docs/reference/v2/tables#resource>`_.

  Examples:

  .. code-block:: yaml

    empty_tables:
      - foo
      - other_dataset.bar
      - other_project:yet_another_dataset.baz

  .. code-block:: yaml

    empty_tables:
      - foo_table_${session_date_compact}
      - id: bar_table_${session_date_compact}
        project: other_project
        dataset: other_dataset
        friendly_name: Bar dataset ${session_date_compact}
        description: Bar dataset for ${session_date}
        expiration_time: 2016-11-01-T01:02:03Z
        schema:
          fields:
            - {name: foo, type: STRING}
            - {name: bar, type: INTEGER}
        labels:
          foo: bar
          quux: 17
        access:
          - domain: example.com
            role: READER
          - userByEmail: ingest@example.com
            role: WRITER
          - groupByEmail: administrators@example.com
            role: OWNER

:command:`delete_tables: LIST`
  Delete tables, if they exist.

  Examples:

  .. code-block:: yaml

    delete_tables:
      - foo
      - other_dataset.bar
      - other_project:yet_another_dataset.baz

  .. code-block:: yaml

    delete_tables:
      - foo_table_${last_session_date_compact}
      - bar_table_${last_session_date_compact}


bq_extract>: Exporting Data from Google BigQuery
------------------------------------------------

The **bq_extract>:** operator can be used to export data from Google BigQuery tables.


.. code-block:: yaml

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

Secrets
~~~~~~~

:command:`gcp.credential: CREDENTIAL`
  See gcp_credential_.

Parameters
~~~~~~~~~~

:command:`bq_extract>: TABLE`
  A reference to the table that should be exported.

  * :command:`bq_extract>: my_table`
  * :command:`bq_extract>: my_dataset.my_table`
  * :command:`bq_extract>: my_project:my_dataset.my_table`

:command:`destination: URI | LIST`
  A URI or list of URIs with the location of the destination export files. These must be Google Cloud Storage URIs.

  Examples:

  .. code-block:: none

    destination: gs://my_bucket/my_export.csv

  .. code-block:: none

    destination:
      - gs://my_bucket/my_export_1.csv
      - gs://my_bucket/my_export_2.csv

:command:`print_header: BOOLEAN`
  Whether to print out a header row in the results. *Default*: ``true``.

:command:`field_delimiter: CHARACTER`
  A delimiter to use between fields in the output. *Default*: ``,``.

  * :command:`field_delimiter: '\\t'`

:command:`destination_format: CSV | NEWLINE_DELIMITED_JSON | AVRO`
  The format of the destination export file. *Default*: ``CSV``.

  * :command:`destination_format: CSV`
  * :command:`destination_format: NEWLINE_DELIMITED_JSON`
  * :command:`destination_format: AVRO`

:command:`compression: GZIP | NONE`
  The compression to use for the export file. *Default*: ``NONE``.

  * :command:`compression: NONE`
  * :command:`compression: GZIP`

Output parameters
~~~~~~~~~~~~~~~~~

:command:`bq.last_job_id`
  The id of the BigQuery job that performed this export.


bq_load>: Importing Data into Google BigQuery
---------------------------------------------

The **bq_load>:** operator can be used to import data into Google BigQuery tables.


.. code-block:: yaml

    _export:
      bq:
        dataset: my_dataset

    +ingest:
      bq_load>: gs://my_bucket/data.csv
      destination_table: my_data

    +process:
      bq>: queries/process.sql
      destination_table: my_result

Secrets
~~~~~~~

:command:`gcp.credential: CREDENTIAL`
  See gcp_credential_.

Parameters
~~~~~~~~~~

:command:`bq_load>: URI | LIST`
  A URI or list of URIs identifying files in GCS to import.

  Examples:

  .. code-block:: yaml

    bq_load>: gs://my_bucket/data.csv


  .. code-block:: yaml

    bq_load>:
      - gs://my_bucket/data1.csv.gz
      - gs://my_bucket/data2_*.csv.gz

:command:`dataset: NAME`
  The dataset that the destination table is located in or should be created in. Can also be specified directly in the table reference.

  * :command:`dataset: my_dataset`
  * :command:`dataset: my_project:my_dataset`

:command:`destination_table: NAME`
  The table to store the imported data in.

  * :command:`destination_table: my_result_table`
  * :command:`destination_table: some_dataset.some_table`
  * :command:`destination_table: some_project:some_dataset.some_table`

:command:`project: NAME`
  The project that the table is located in or should be created in. Can also be specified directly in the table reference or the dataset parameter.

:command:`source_format: CSV | NEWLINE_DELIMITED_JSON | AVRO | DATASTORE_BACKUP`
  The format of the files to be imported. *Default*: ``CSV``.

  * :command:`source_format: CSV`
  * :command:`source_format: NEWLINE_DELIMITED_JSON`
  * :command:`source_format: AVRO`
  * :command:`source_format: DATASTORE_BACKUP`

:command:`field_delimiter: CHARACTER`
  The separator used between fields in CSV files to be imported. *Default*: ``,``.

  * :command:`field_delimiter: '\\t'`

:command:`create_disposition: CREATE_IF_NEEDED | CREATE_NEVER`
  Specifies whether the destination table should be automatically created when performing the import.

  - ``CREATE_IF_NEEDED``: *(default)* The destination table is created if it does not already exist.
  - ``CREATE_NEVER``: The destination table must already exist, otherwise the import will fail.

  Examples:

  * :command:`create_disposition: CREATE_IF_NEEDED`
  * :command:`create_disposition: CREATE_NEVER`

:command:`write_disposition: WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY`
  Specifies whether to permit importing data to an already existing destination table.

  - ``WRITE_TRUNCATE``: If the destination table already exists, any data in it will be overwritten.
  - ``WRITE_APPEND``: If the destination table already exists, any data in it will be appended to.
  - ``WRITE_EMPTY``: *(default)* The import fails if the destination table already exists and is not empty.

  Examples:

  * :command:`write_disposition: WRITE_TRUNCATE`
  * :command:`write_disposition: WRITE_APPEND`
  * :command:`write_disposition: WRITE_EMPTY`

:command:`skip_leading_rows: INTEGER`
  The number of leading rows to skip in CSV files to import. *Default*: ``0``.

  * :command:`skip_leading_rows: 1`

:command:`encoding: UTF-8 | ISO-8859-1`
  The character encoding of the data in the files to import. *Default*: ``UTF-8``.

  * :command:`encoding: ISO-8859-1`

:command:`quote: CHARACTER`
  The character quote of the data in the files to import. *Default*: ``'"'``.

  * :command:`quote: ''`
  * :command:`quote: "'"`

:command:`max_bad_records: INTEGER`
  The maximum number of bad records to ignore before failing the import. *Default*: ``0``.

  * :command:`max_bad_records: 100`

:command:`allow_quoted_newlines: BOOLEAN`
  Whether to allow quoted data sections that contain newline characters in a CSV file. *Default*: ``false``.

:command:`allow_jagged_rows: BOOLEAN`
  Whether to accept rows that are missing trailing optional columns in CSV files. *Default*: ``false``.

:command:`ignore_unknown_values: BOOLEAN`
  Whether to ignore extra values in data that are not represented in the table schema. *Default*: ``false``.

:command:`projection_fields: LIST`
  A list of names of Cloud Datastore entity properties to load. Requires ``source_format: DATASTORE_BACKUP``.

:command:`autodetect: BOOLEAN`
  Whether to automatically infer options and schema for CSV and JSON sources. *Default*: ``false``.

:command:`schema_update_options: LIST`
  A list of destination table schema updates that may be automatically performed when performing the import.

  .. code-block:: yaml

    schema_update_options:
      - ALLOW_FIELD_ADDITION
      - ALLOW_FIELD_RELAXATION

Output parameters
~~~~~~~~~~~~~~~~~

:command:`bq.last_job_id`
  The id of the BigQuery job that performed this import.

gcs_wait>: Wait for a file in Google Cloud Storage
--------------------------------------------------

The **gcs_wait>:** operator can be used to wait for file to appear in Google Cloud Storage.


.. code-block:: yaml

    +wait:
      gcs_wait>: my_bucket/some/file

    +wait:
      gcs_wait>: gs://my_bucket/some/file

Secrets
~~~~~~~

:command:`gcp.credential: CREDENTIAL`
  See gcp_credential_.

Parameters
~~~~~~~~~~

:command:`gcs_wait>: URI | BUCKET/OBJECT`
  Google Cloud Storage URI or path of the file to wait for.

  * :command:`gcs_wait>: my-bucket/my-directory/my-data.gz`
  * :command:`gcs_wait>: gs://my-bucket/my-directory/my-data.gz`

:command:`bucket: NAME`
  The GCS bucket where the file is located. Can be used together with the `object` parameter instead of putting the path on the operator command line.

:command:`object: PATH`
  The GCS path of the file. Can be used together with the `bucket` parameter instead of putting the path on the operator command line.


Output parameters
~~~~~~~~~~~~~~~~~

:command:`gcs_wait.last_object`
  Information about the detected file.

    .. code-block:: yaml

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

.. note:: The **gcs_wait>:** operator makes use of polling with *exponential backoff*. As such there might be some time interval between a file being created and the **gcs_wait>:** operator detecting it.


http>: Making HTTP requests
---------------------------

The **http>:** operator can be used to make HTTP requests.

.. code-block:: yaml

    +fetch:
      http>: https://api.example.com/foobars
      store_content: true

    +process:
      for_each>:
        foobar: ${http.last_content}
      _do:
        bq>: query.sql

.. code-block:: yaml

    +notify:
      http>: https://api.example.com/data/sessions/{$session_uuid}
      method: POST
      content:
        status: RUNNING
        time: ${session_time}

Secrets
~~~~~~~

:command:`http.authorization: STRING`
  A string that should be included in the HTTP request as the value of the ``Authorization`` header. This can be used to authenticate using e.g. Oauth bearer tokens.

:command:`http.user: STRING`
  A user that should be used to authenticate using *Basic Authentication*.

:command:`http.password: STRING`
  A password that should be used to authenticate using *Basic Authentication*.

:command:`http.uri: URI`
  The URI of the HTTP request. This can be used instead of putting the URI on the operator command line in case the URI contains sensitive information.

Parameters
~~~~~~~~~~

:command:`http>: URI`
  The URI of the HTTP request.

  * :command:`http>: https://api.example.com/foobar`
  * :command:`http>: https://api.example.com/data/sessions/{$session_uuid}`

:command:`method: STRING`
  The method of the HTTP request. *Default:* ``GET``.

  * :command:`method: POST`
  * :command:`method: DELETE`

:command:`content: STRING | INTEGER | BOOLEAN | OBJECT | ARRAY`
  The content of the HTTP request. *Default:* No content.

  Scalars (i.e. strings, integers, booleans, etc) will by default be sent as plain text. Objects and arrays will by default be JSON serialized. The ``content_format`` parameter can be used to control the content serialization format.

  .. code-block:: yaml

    content: 'hello world'

  .. code-block:: yaml

    content: '${session_time}'

  .. code-block:: yaml

    content:
      status: RUNNING
      time: ${session_time}

:command:`content_format: text | json | form`
  The serialization format of the content of the HTTP request. *Default:* Inferred from the ``content`` parameter value type. Objects and arrays use ``json`` by default. Other value types default to ``text``.

  - ``text``: Send raw content as ``Content-Type: text/plain``. *Note:* This requires that the ``content`` parameter is _not_ array or an object.
  - ``json``: Serialize the content as `JSON <http://json.org/>`_ and send it as ``Content-Type: application/json``. This format can handle any ``content`` parameter value type.
  - ``form``: Encode content as an HTML form and send it as ``Content-Type: application/x-www-form-urlencoded``. *Note:* This requires the ``content`` parameter value to be an object.

  .. code-block:: yaml

    content: 'hello world @ ${session_time}'
    content_format: text

  .. code-block:: yaml

    content:
      status: RUNNING
      time: ${session_time}
    content_format: json

  .. code-block:: yaml

    content:
      status: RUNNING
      time: ${session_time}
    content_format: form

:command:`content_type: STRING`
  Override the inferred ``Content-Type`` header.

  .. code-block:: yaml

    content: |
      <?xml version="1.0" encoding="UTF-8"?>
      <notification>
        <status>RUNNING</status>
        <time>${session_time}</time>
      </notification>
    content_format: text
    content_type: application/xml

:command:`store_content: BOOLEAN`
  Whether to store the content of the response. *Default:* ``false``.

:command:`headers: LIST OF KEY-VALUE PAIRS`
  Additional custom headers to send with the HTTP request.

  .. code-block:: yaml

    headers:
      - Accept: application/json
      - X-Foo: bar
      - Baz: quux

:command:`retry: BOOLEAN`
  Whether to retry ephemeral errors. *Default:* ``true`` if the request method is ``GET``, ``HEAD``, ``OPTIONS`` or ``TRACE``. Otherwise ``false``.

  Client ``4xx`` errors (except for ``408 Request Timeout`` and ``429 Too Many Requests``) will not be retried even if ``retry`` is set to ``true``.

  *Note:* Enabling retries might cause the target endpoint to receive multiple duplicate HTTP requests. Thus retries should only be enabled if duplicated requests are tolerable. E.g. when the outcome of the HTTP request is *idempotent*.

