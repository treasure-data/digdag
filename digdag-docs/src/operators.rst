Operators
==================================

.. contents::
   :local:
   :depth: 2

require>: Runs another workflow
----------------------------------

**require>:** operator runs another workflow. It's skipped if the workflow is already done successfully.

.. code-block:: yaml

    run: +main
    +main:
      require>: +another
    +another:
      sh>: tasks/another.sh

:command:`require>: +NAME`
  Name of a workflow.

  Example: +another_task

py>: Python scripts
----------------------------------

**py>:** operator runs a Python script using ``python`` command.
TODO: link to `Python API documents <ruby_api.html>`_ for details including variable mappings to keyword arguments.

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

TODO: add more description here
TODO: link to `Ruby API documents <python_api.html>`_ for details including best practices how to configure the workflow using ``export: require:``.

.. code-block:: yaml

    _export:
      ruby:
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

TODO: add more description here

.. code-block:: yaml

    +step1:
      sh>: tasks/step1.sh
    +step2:
      sh>: tasks/step2.sh

:command:`sh>: COMMAND [ARGS...]`
  Name of the command to run.

  * :command:`sh>: tasks/workflow.sh --task1`


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
  Variables used for the loop in ``key: [value, value, ...]`` syntax.

  * :command:`for_each>: {i: [1, 2, 3]}`

:command:`_parallel: BOOLEAN`
  Runs the repeating tasks in parallel.

  * :command:`_parallel: true`

:command:`_do: TASKS`
  Tasks to run.


td>: Treasure Data queries
----------------------------------

**td>:** operator runs a Hive or Presto query on Treasure Data.

TODO: add more description here

.. code-block:: yaml

    _export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +step1:
      td>: queries/step1.sql
    +step2:
      td>: queries/step2.sql
      create_table: mytable_${session_date_compact}
    +step3:
      td>: queries/step2.sql
      insert_into: mytable

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

:command:`apikey: APIKEY`
  API key. You can set this at command line using ``-p td.apikey=$TD_APIKEY`` argument.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

:command:`endpoint: ADDRESS`
  API endpoint (default: api.treasuredata.com).

:command:`use_ssl: BOOLEAN`
  Enable SSL (https) to access to the endpoint (default: true).

:command:`engine: presto`
  Query engine (``presto`` or ``hive``).

  * :command:`engine: hive`
  * :command:`engine: presto`


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
        apikey: YOUR/API_KEY
        database: www_access

    +step1:
      td_run>: myquery1
    +step2:
      td_run>: myquery2
      session_time: 2016-01-01T01:01:01+0000

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

:command:`apikey: APIKEY`
  API key. You can set this at command line using ``-p td.apikey=$TD_APIKEY`` argument.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

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

    _export:
      td:
        apikey: YOUR/API_KEY

    +step1:
      td_load>: config/guessed.yml
      database: prod
      table: raw

:command:`td_load>: FILE.yml`
  Path to a YAML template file. This configuration needs to be guessed using td command.

  * :command:`td>: config/from_s3.sql`

:command:`database: NAME`
  Name of the database load data to.

  * :command:`database: my_database`

:command:`table: NAME`
  Name of the table load data to.

  * :command:`table: my_table`

:command:`apikey: APIKEY`
  API key. You can set this at command line using ``-p td.apikey=$TD_APIKEY`` argument.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

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

**_type: td_ddl** operator runs an operational task on Treasure Data.

TODO: add more description here

.. code-block:: yaml

    _export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +step1:
      _type: td_ddl
      create_tables: ["my_table_${session_date_compact}"]
    +step2:
      _type: td_ddl
      drop_tables: ["my_table_${session_date_compact}"]
    +step2:
      _type: td_ddl
      empty_tables: ["my_table_${session_date_compact}"]

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

:command:`apikey: APIKEY`
  API key. You can set this at command line using ``-p td.apikey=$TD_APIKEY`` argument.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

:command:`endpoint: ADDRESS`
  API endpoint (default: api.treasuredata.com).

:command:`use_ssl: BOOLEAN`
  Enable SSL (https) to access to the endpoint (default: true).


td_table_export>: Treasure Data table export to S3
----------------------------------

**td_table_export>:** operator loads data from storages, databases, or services.

TODO: add more description here

.. code-block:: yaml

    _export:
      td:
        apikey: YOUR/API_KEY

    +step1:
      _type: td_table_export
      database: mydb
      table: mytable
      file_format: jsonl.gz
      from: 2016-01-01 00:00:00 +0800
      to:   2016-02-01 00:00:00 +0800
      s3_bucket: my_backup_backet
      s3_path_prefix: mydb/mytable
      s3_access_key_id: ABCDEFGHJKLMNOPQRSTU
      s3_secret_access_key: QUtJ/QUpJWTQ3UkhZTERNUExTUEEQUtJQUpJWTQ3

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

:command:`s3_access_key_id: KEY`
  S3 access key id.

  * :command:`s3_access_key_id: ABCDEFGHJKLMNOPQRSTU`

:command:`s3_secret_access_key: KEY`
  S3 secret access key.

  * :command:`s3_secret_access_key: QUtJ/QUpJWTQ3UkhZTERNUExTUEEQUtJQUpJWTQ3`

:command:`apikey: APIKEY`
  API key. You can set this at command line using ``-p td.apikey=$TD_APIKEY`` argument.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

:command:`endpoint: ADDRESS`
  API endpoint (default: api.treasuredata.com).

:command:`use_ssl: BOOLEAN`
  Enable SSL (https) to access to the endpoint (default: true).


Output parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td.last_job_id`
  The job id this task executed.

  * :command:`52036074`


mail>: Sending email
----------------------------------

**mail>:** operator sends an email.

To use Gmail SMTP server, you need to do either of:

  a) Generate a new app password at `App passwords <https://security.google.com/settings/security/apppasswords>`_. This needs to enable 2-Step Verification first.

  b) Enable access for less secure apps at `Less secure apps <https://www.google.com/settings/security/lesssecureapps>`_. This works even if 2-Step Verification is not enabled.

.. code-block:: yaml

    _export:
      mail:
        host: smtp.gmail.com
        port: 587
        from: "you@gmail.com"
        username: "you@gmail.com"
        password: "...password..."
        debug: true

    +step1:
      mail>: body.txt
      subject: workflow started
      to: [me@example.com]

    +step2:
      _type: mail
      body: this is email body in string
      subject: workflow started
      to: [me@example.com]

    +step3:
      sh>: this_task_might_fail.sh
      error:
        mail>: body.txt
        subject: this workflow failed
        to: [me@example.com]

:command:`mail>: FILE`
  Path to a mail body template file. This file can contain ``${...}`` syntax to embed variables.

  * :command:`mail>: mail_body.txt`

:command:`subject: SUBJECT`
  Subject of the email.

  * :command:`subject: Mail From Digdag`

:command:`body: TEXT`
  Email body if tempalte file path is not set.

  * :command:`body: Hello, this is from Digdag`

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
  SMTP login username if authentication is required me.

  * :command:`username: me`

:command:`password: PASSWORD`
  SMTP login password.

  * :command:`password: MyPaSsWoRd`

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

