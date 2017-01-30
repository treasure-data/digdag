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

Examples
~~~~~~~~

  * `Examples <https://github.com/treasure-data/workflow-examples/tree/master/td>`_

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

.. code-block:: yaml

    _export:
      td:
        database: www_access

    +step1:
      td_run>: 12345
    +step2:
      td_run>: myquery2
      session_time: 2016-01-01T01:01:01+0000

Examples
~~~~~~~~

  * `Examples <https://github.com/treasure-data/workflow-examples/tree/master/td_run>`_

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when running Treasure Data queries.

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td_run>: SAVED_QUERY_ID or SAVED_QUERY_NAME`
  Runs saved query. If number was specified, it's considered as an ID of saved query. Otherwise it's considered as a name of a saved query.

  * :command:`td_run>: 12345`
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


td_for_each>: Repeat using Treasure Data queries
----------------------------------

**td_for_each>:** operator loops subtasks for each result rows of a Hive or Presto query on Treasure Data.

Subtasks set at ``_do`` section can reference results using ${td.each.COLUMN_NAME} syntax where COLUMN_NAME is a name of column.

For example, if you run a query ``select email, name from users`` and the query returns 3 rows, this operator runs subtasks 3 times with ``${td.each.email}`` and ``${td.each.name}}`` parameters.

.. code-block:: yaml

    _export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +for_each_users:
      td_for_each>: queries/users.sql
      _do:
        +show:
          echo>: found a user ${td.each.name} email ${td.each.email}

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when running Treasure Data queries.

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td>: FILE.sql`
  Path to a query template file. This file can contain ``${...}`` syntax to embed variables.

  * :command:`td>: queries/step1.sql`

:command:`database: NAME`
  Name of a database.

  * :command:`database: my_db`

:command:`apikey: APIKEY`
  API key. This must be set as a secret parameter.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

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


td_wait_table>: Waits for data arriving at Treasure Data table
----------------------------------

**td_wait_table>:** operator checks a table periodically until it has certain number of records in a configured range. This is useful to wait execution of following tasks until some records are imported to a table.

.. code-block:: yaml

    _export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +wait:
      td_wait_table>: target_table

    +step1:
      td>: queries/use_records.sql

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when running Treasure Data queries.

Parameters
~~~~~~~~~~

:command:`td_wait_table>: FILE.sql`
  Name of a table.

  * :command:`td_wait_table>: target_table`

:command:`rows: N`
  Number of rows to wait (default: 0).

  * :command:`rows: 10`

:command:`database: NAME`
  Name of a database.

  * :command:`database: my_db`

:command:`apikey: APIKEY`
  API key. This must be set as a secret parameter.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

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



td_wait>: Waits for data arriving at Treasure Data table
----------------------------------

**td_wait>:** operator runs a query periodically until it returns true. This operator can use more complex query compared to ``td_wait_table>:`` operator

.. code-block:: yaml

    _export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +wait:
      td_wait>: queries/check_recent_record.sql

    +step1:
      td>: queries/use_records.sql

Example queries:

.. code-block:: sql

    select 1 from target_table where TD_TIME_RANGE(time, '${session_time}') limit 1

    select count(*) > 1000 from target_table where TD_TIME_RANGE(time, '${last_session_time}')

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when running Treasure Data queries.

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td_wait>: FILE.sql`
  Path to a query template file. This file can contain ``${...}`` syntax to embed variables.

  * :command:`td_wait>: queries/check_recent_record.sql`

:command:`database: NAME`
  Name of a database.

  * :command:`database: my_db`

:command:`apikey: APIKEY`
  API key. This must be set as a secret parameter.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

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


td_load>: Treasure Data bulk loading
----------------------------------

**td_load>:** operator loads data from storages, databases, or services.

.. code-block:: yaml

    +step1:
      td_load>: config/guessed.yml
      database: prod
      table: raw

Examples
~~~~~~~~

  * `Examples <https://github.com/treasure-data/workflow-examples/tree/master/td_load>`_

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
    +step3:
      td_ddl>:
      empty_tables: ["my_table_${session_date_compact}"]
    +step4:
      td_ddl>:
      rename_tables: [{from: "my_table_${session_date_compact}", to: "my_table"}]

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

:command:`rename_tables: [ARRAY OF {to:, from:}]`
  Rename a table to another name (override the destination table if it already exists).

  * :command:`rename_tables: [{from: my_table1, to: my_table2}]`

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


td_partial_delete>: Delete range of Treasure Data table
----------------------------------

**td_partial_delete>:** operator deletes records from a Treasure Data table.

Please be aware that records imported using streaming import can't be deleted for several hours using td_partial_delete. Records imported by INSERT INTO, Data Connector, and bulk imports can be deleted immediately.

Time range needs to be hourly. Setting non-zero values to minutes or seconds will be rejected.

.. code-block:: yaml

    _export:
      td:
        apikey: YOUR/API_KEY

    +step1:
      td_partial_delete>:
      database: mydb
      table: mytable
      from: 2016-01-01 00:00:00 +0800
      to:   2016-02-01 00:00:00 +0800

Secrets
~~~~~~~

:command:`td.apikey: API_KEY`
  The Treasure Data API key to use when running Treasure Data queries.

Parameters
~~~~~~~~~~

:command:`database: NAME`
  Name of the database.

  * :command:`database: my_database`

:command:`table: NAME`
  Name of the table to export.

  * :command:`table: my_table`

:command:`from: yyyy-MM-dd HH:mm:ss[ Z]`
  Delete records from this time (inclusive). Actual time range is :command:`[from, to)`. Value should be a UNIX timestamp integer (seconds) or string in yyyy-MM-dd HH:mm:ss[ Z] format.

  * :command:`from: 2016-01-01 00:00:00 +0800`

:command:`to: yyyy-MM-dd HH:mm:ss[ Z]`
  Delete records to this time (exclusive). Actual time range is :command:`[from, to)`. Value should be a UNIX timestamp integer (seconds) or string in yyyy-MM-dd HH:mm:ss[ Z] format.

  * :command:`to: 2016-02-01 00:00:00 +0800`

:command:`apikey: APIKEY`
  API key. This must be set as a secret parameter.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

:command:`endpoint: ADDRESS`
  API endpoint (default: api.treasuredata.com).

:command:`use_ssl: BOOLEAN`
  Enable SSL (https) to access to the endpoint (default: true).


td_table_export>: Treasure Data table export to S3
----------------------------------

**td_table_export>:** operator loads data from storages, databases, or services.

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

:command:`aws.s3.access_key_id: ACCESS_KEY_ID`
  The AWS Access Key ID to use when writing to S3.

  * :command:`aws.s3.access_key_id: ABCDEFGHJKLMNOPQRSTU`

:command:`aws.s3.secret_access_key: SECRET_ACCESS_KEY`
  The AWS Secret Access Key to use when writing to S3.

  * :command:`aws.s3.secret_access_key: QUtJ/QUpJWTQ3UkhZTERNUExTUEEQUtJQUpJWTQ3`


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
        schema: myschema
        # strict_transaction: false

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
  Optional user password to use when connecting to the postgres database.

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
  Port number to connect to the database. *Default*: ``5432``.

  * :command:`port: 2345`

:command:`user: NAME`
  User to connect to the database

  * :command:`user: app_user`

:command:`ssl: BOOLEAN`
  Enable SSL to connect to the database. *Default*: ``false``.

  * :command:`ssl: true`

:command:`schema: NAME`
  Default schema name. *Default*: ``public``.

  * :command:`schema: my_schema`

:command:`strict_transaction: BOOLEAN`
  Whether this operator uses a strict transaction to prevent generating unexpected duplicated records just in case. *Default*: ``true``.
  This operator creates and uses a status table in the database to make an operation idempotent. But if creating a table isn't allowed, this option should be false.

  * :command:`strict_transaction: false`

:command:`status_table_schema: NAME`
  Schema name of status table. *Default*: same as the value of ``schema`` option.

  * :command:`status_table_schema: writable_schema`

:command:`status_table: NAME`
  Table name of status table. *Default*: ``__digdag_status``.

  * :command:`status_table: customized_status_table`


redshift>: Redshift operations
----------------------------------

**redshift>** operator runs queries and/or DDLs on Redshift

.. code-block:: yaml

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


Secrets
~~~~~~~

:command:`aws.redshift.password: NAME`
  Optional user password to use when connecting to the Redshift database.

Parameters
~~~~~~~~~~

:command:`redshift>: FILE.sql`
  Path of the query template file. This file can contain ``${...}`` syntax to embed variables.

  * :command:`redshift>: queries/complex_queries.sql`

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
  Port number to connect to the database. *Default*: ``5439``.

  * :command:`port: 2345`

:command:`user: NAME`
  User to connect to the database

  * :command:`user: app_user`

:command:`ssl: BOOLEAN`
  Enable SSL to connect to the database. *Default*: ``false``.

  * :command:`ssl: true`

:command:`schema: NAME`
  Default schema name. *Default*: ``public``.

  * :command:`schema: my_schema`

:command:`strict_transaction: BOOLEAN`
  Whether this operator uses a strict transaction to prevent generating unexpected duplicated records just in case. *Default*: ``true``.
  This operator creates and uses a status table in the database to make an operation idempotent. But if creating a table isn't allowed, this option should be false.

  * :command:`strict_transaction: false`

:command:`status_table_schema: NAME`
  Schema name of status table. *Default*: same as the value of ``schema`` option.

  * :command:`status_table_schema: writable_schema`

:command:`status_table: NAME`
  Table name prefix of status table. *Default*: ``__digdag_status``.

  * :command:`status_table: customized_status_table`


redshift_load>: Redshift load operations
----------------------------------

**redshift_load>** operator runs COPY statement to load data from external storage on Redshift

.. code-block:: yaml

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
        column_list: host, path, referer, code, agent, size, method
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


Secrets
~~~~~~~

:command:`aws.redshift.password: NAME`
  Optional user password to use when connecting to the Redshift database.

:command:`aws.redshift_load.access_key_id, aws.redshift.access_key_id, aws.access_key_id`
  The AWS Access Key ID to use when accessing data source. This value is used to get temporary security credentials by default. See `temp_credentials` option for details.

:command:`aws.redshift_load.secret_access_key, aws.redshift.secret_access_key, aws.secret_access_key`
  The AWS Secret Access Key to use when accessing data source. This value is used to get temporary security credentials by default. See `temp_credentials` option for details.

:command:`aws.redshift_load.role_arn, aws.redshift.role_arn, aws.role_arn`
  Optional Amazon resource names (ARNs) used to copy data to the Redshift. The role needs `AssumeRole` role to use this option. Requires ``temp_credentials`` to be true.
  If this option isn't specified, this operator tries to use a federated user


Parameters
~~~~~~~~~~

:command:`database: NAME`
  Database name.

  * :command:`database: my_db`

:command:`host: NAME`
  Hostname or IP address of the database.

  * :command:`host: db.foobar.com`

:command:`port: NUMBER`
  Port number to connect to the database. *Default*: ``5439``.

  * :command:`port: 2345`

:command:`user: NAME`
  User to connect to the database

  * :command:`user: app_user`

:command:`ssl: BOOLEAN`
  Enable SSL to connect to the database. *Default*: ``false``.

  * :command:`ssl: true`

:command:`schema: NAME`
  Default schema name. *Default*: ``public``.

  * :command:`schema: my_schema`

:command:`strict_transaction: BOOLEAN`
  Whether this operator uses a strict transaction to prevent generating unexpected duplicated records just in case. *Default*: ``true``.
  This operator creates and uses a status table in the database to make an operation idempotent. But if creating a table isn't allowed, this option should be false.

  * :command:`strict_transaction: false`

:command:`status_table_schema: NAME`
  Schema name of status table. *Default*: same as the value of ``schema`` option.

  * :command:`status_table_schema: writable_schema`

:command:`status_table: NAME`
  Table name prefix of status table. *Default*: ``__digdag_status``.

  * :command:`status_table: customized_status_table`

:command:`table: NAME`
  Table name in Redshift database to be loaded data

  * :command:`table: access_logs`

:command:`from: URI`
  Parameter mapped to `FROM` parameter of Redshift`s `COPY` statement

  * :command:`from: s3://my-app-bucket/access_logs/today`

:command:`column_list: CSV`
  Parameter mapped to `COLUMN_LIST` parameter of Redshift`s `COPY` statement

  * :command:`column_list: host, path, referer, code, agent, size, method`

:command:`manifest: BOOLEAN`
  Parameter mapped to `MANIFEST` parameter of Redshift`s `COPY` statement

  * :command:`manifest: true`

:command:`encrypted: BOOLEAN`
  Parameter mapped to `ENCRYPTED` parameter of Redshift`s `COPY` statement

  * :command:`encrypted: true`

:command:`readratio: NUMBER`
  Parameter mapped to `READRATIO` parameter of Redshift`s `COPY` statement

  * :command:`readratio: 150`

:command:`region: NAME`
  Parameter mapped to `REGION` parameter of Redshift`s `COPY` statement

  * :command:`region: us-east-1`

:command:`csv: CHARACTER`
  Parameter mapped to `CSV` parameter of Redshift`s `COPY` statement.
  If you want to just use default quote charactor of `CSV` parameter, set empty string like `csv: ''`

  * :command:`csv: "'"`

:command:`delimiter: CHARACTER`
  Parameter mapped to `DELIMITER` parameter of Redshift`s `COPY` statement

  * :command:`delimiter: "$"`

:command:`json: URI`
  Parameter mapped to `JSON` parameter of Redshift`s `COPY` statement

  * :command:`json: auto`
  * :command:`json: s3://my-app-bucket/access_logs/jsonpathfile`

:command:`avro: URI`
  Parameter mapped to `AVRO` parameter of Redshift`s `COPY` statement

  * :command:`avro: auto`
  * :command:`avro: s3://my-app-bucket/access_logs/jsonpathfile`

:command:`fixedwidth: CSV`
  Parameter mapped to `FIXEDWIDTH` parameter of Redshift`s `COPY` statement

  * :command:`fixedwidth: host:15,code:3,method:15`

:command:`gzip: BOOLEAN`
  Parameter mapped to `GZIP` parameter of Redshift`s `COPY` statement

  * :command:`gzip: true`

:command:`bzip2: BOOLEAN`
  Parameter mapped to `BZIP2` parameter of Redshift`s `COPY` statement

  * :command:`bzip2: true`

:command:`lzop: BOOLEAN`
  Parameter mapped to `LZOP` parameter of Redshift`s `COPY` statement

  * :command:`lzop: true`

:command:`acceptanydate: BOOLEAN`
  Parameter mapped to `ACCEPTANYDATE` parameter of Redshift`s `COPY` statement

  * :command:`acceptanydate: true`

:command:`acceptinvchars: CHARACTER`
  Parameter mapped to `ACCEPTINVCHARS` parameter of Redshift`s `COPY` statement

  * :command:`acceptinvchars: "&"`

:command:`blanksasnull: BOOLEAN`
  Parameter mapped to `BLANKSASNULL` parameter of Redshift`s `COPY` statement

  * :command:`blanksasnull: true`

:command:`dateformat: STRING`
  Parameter mapped to `DATEFORMAT` parameter of Redshift`s `COPY` statement

  * :command:`dateformat: yyyy-MM-dd`

:command:`emptyasnull: BOOLEAN`
  Parameter mapped to `EMPTYASNULL` parameter of Redshift`s `COPY` statement

  * :command:`emptyasnull: true`

:command:`encoding: TYPE`
  Parameter mapped to `ENCODING` parameter of Redshift`s `COPY` statement

  * :command:`encoding: UTF8`

:command:`escape: BOOLEAN`
  Parameter mapped to `ESCAPE` parameter of Redshift`s `COPY` statement

  * :command:`escape: false`

:command:`explicit_ids: BOOLEAN`
  Parameter mapped to `EXPLICIT_IDS` parameter of Redshift`s `COPY` statement

  * :command:`explicit_ids: true`

:command:`fillrecord: BOOLEAN`
  Parameter mapped to `FILLRECORD` parameter of Redshift`s `COPY` statement

  * :command:`fillrecord: true`

:command:`ignoreblanklines: BOOLEAN`
  Parameter mapped to `IGNOREBLANKLINES` parameter of Redshift`s `COPY` statement

  * :command:`ignoreblanklines: true`

:command:`ignoreheader: NUMBER`
  Parameter mapped to `IGNOREHEADER` parameter of Redshift`s `COPY` statement

  * :command:`ignoreheader: 2`

:command:`null_as: STRING`
  Parameter mapped to `NULL AS` parameter of Redshift`s `COPY` statement

  * :command:`null_as: nULl`

:command:`removequotes: BOOLEAN`
  Parameter mapped to `REMOVEQUOTES` parameter of Redshift`s `COPY` statement

  * :command:`removequotes: false`

:command:`roundec: BOOLEAN`
  Parameter mapped to `ROUNDEC` parameter of Redshift`s `COPY` statement

  * :command:`roundec: true`

:command:`timeformat: STRING`
  Parameter mapped to `TIMEFORMAT` parameter of Redshift`s `COPY` statement

  * :command:`timeformat: YYYY-MM-DD HH:MI:SS`

:command:`trimblanks: BOOLEAN`
  Parameter mapped to `TRIMBLANKS` parameter of Redshift`s `COPY` statement

  * :command:`trimblanks: true`

:command:`truncatecolumns: BOOLEAN`
  Parameter mapped to `TRUNCATECOLUMNS` parameter of Redshift`s `COPY` statement

  * :command:`truncatecolumns: true`

:command:`comprows: NUMBER`
  Parameter mapped to `COMPROWS` parameter of Redshift`s `COPY` statement

  * :command:`comprows: 12`

:command:`compupdate: TYPE`
  Parameter mapped to `COMPUPDATE` parameter of Redshift`s `COPY` statement

  * :command:`compupdate: ON`

:command:`maxerror: NUMBER`
  Parameter mapped to `MAXERROR` parameter of Redshift`s `COPY` statement

  * :command:`maxerror: 34`

:command:`noload: BOOLEAN`
  Parameter mapped to `NOLOAD` parameter of Redshift`s `COPY` statement

  * :command:`noload: true`

:command:`statupdate: TYPE`
  Parameter mapped to `STATUPDATE` parameter of Redshift`s `COPY` statement

  * :command:`statupdate: off`

:command:`temp_credentials`
  Whether this operator uses temporary security credentials. *Default*: ``true``.
  This operator tries to use temporary security credentials as follows:
    - If `role_arn` is specified, it calls `AssumeRole` action
    - If not, it calls `GetFederationToken` action

  See details about `AssumeRole` and `GetFederationToken` in the documents of AWS Security Token Service.

  So either of `AssumeRole` or `GetFederationToken` action is called to use temporary security credentials by default for secure operation.
  But if this option is disabled, this operator uses credentials as-is set in the secrets insread of temporary security credentials.

  * :command:`temp_credentials: false`

:command:`session_duration INTEGER`
  Session duration of temporary security credentials. *Default*: ``3 hour``.
  This option isn't used when disabling `temp_credentials`

  * :command:`session_duration: 1800`
        

redshift_unload>: Redshift load operations
----------------------------------

**redshift_unload>** operator runs UNLOAD statement to export data to external storage on Redshift

.. code-block:: yaml

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
        parallel: true

Secrets
~~~~~~~

:command:`aws.redshift.password: NAME`
  Optional user password to use when connecting to the Redshift database.

:command:`aws.redshift_unload.access_key_id, aws.redshift.access_key_id, aws.access_key_id`
  The AWS Access Key ID to use when accessing data source. This value is used to get temporary security credentials by default. See `temp_credentials` option for details.

:command:`aws.redshift_unload.secret_access_key, aws.redshift.secret_access_key, aws.secret_access_key`
  The AWS Secret Access Key to use when accessing data source. This value is used to get temporary security credentials by default. See `temp_credentials` option for details.

:command:`aws.redshift_load.role_arn, aws.redshift.role_arn, aws.role_arn`
  Optional Amazon resource names (ARNs) used to copy data to the Redshift. The role needs `AssumeRole` role to use this option. Requires ``temp_credentials`` to be true.
  If this option isn't specified, this operator tries to use a federated user


Parameters
~~~~~~~~~~

:command:`database: NAME`
  Database name.

  * :command:`database: my_db`

:command:`host: NAME`
  Hostname or IP address of the database.

  * :command:`host: db.foobar.com`

:command:`port: NUMBER`
  Port number to connect to the database. *Default*: ``5439``.

  * :command:`port: 2345`

:command:`user: NAME`
  User to connect to the database

  * :command:`user: app_user`

:command:`ssl: BOOLEAN`
  Enable SSL to connect to the database. *Default*: ``false``.

  * :command:`ssl: true`

:command:`schema: NAME`
  Default schema name. *Default*: ``public``.

  * :command:`schema: my_schema`

:command:`strict_transaction: BOOLEAN`
  Whether this operator uses a strict transaction to prevent generating unexpected duplicated records just in case. *Default*: ``true``.
  This operator creates and uses a status table in the database to make an operation idempotent. But if creating a table isn't allowed, this option should be false.

  * :command:`strict_transaction: false`

:command:`status_table_schema: NAME`
  Schema name of status table. *Default*: same as the value of ``schema`` option.

  * :command:`status_table_schema: writable_schema`

:command:`status_table: NAME`
  Table name prefix of status table. *Default*: ``__digdag_status``.

  * :command:`status_table: customized_status_table`

:command:`query: STRING`
  SELECT query. The results of the query are unloaded.

  * :command:`query: select * from access_logs`

:command:`to: URI`
  Parameter mapped to `TO` parameter of Redshift`s `UNLOAD` statement

  * :command:`to: s3://my-app-bucket/access_logs/today`

manifest
:command:`manifest: BOOLEAN`
  Parameter mapped to `MANIFEST` parameter of Redshift`s `UNLOAD` statement

  * :command:`manifest: true`

encrypted
:command:`encrypted: BOOLEAN`
  Parameter mapped to `ENCRYPTED` parameter of Redshift`s `UNLOAD` statement

  * :command:`encrypted: true`

allowoverwrite
:command:`allowoverwrite: BOOLEAN`
  Parameter mapped to `ALLOWOVERWRITE` parameter of Redshift`s `UNLOAD` statement

  * :command:`allowoverwrite: true`

delimiter
:command:`delimiter: CHARACTER`
  Parameter mapped to `DELIMITER` parameter of Redshift`s `UNLOAD` statement

  * :command:`delimiter: "$"`

fixedwidth
:command:`fixedwidth: BOOLEAN`
  Parameter mapped to `FIXEDWIDTH` parameter of Redshift`s `UNLOAD` statement

  * :command:`fixedwidth: host:15,code:3,method:15`

gzip
:command:`gzip: BOOLEAN`
  Parameter mapped to `GZIP` parameter of Redshift`s `UNLOAD` statement

  * :command:`gzip: true`

bzip2
:command:`bzip2: BOOLEAN`
  Parameter mapped to `BZIP2` parameter of Redshift`s `UNLOAD` statement

  * :command:`bzip2: true`

null_as
:command:`null_as: BOOLEAN`
  Parameter mapped to `NULL_AS` parameter of Redshift`s `UNLOAD` statement

  * :command:`null_as: nuLL`

escape
:command:`escape: BOOLEAN`
  Parameter mapped to `ESCAPE` parameter of Redshift`s `UNLOAD` statement

  * :command:`escape: true`

addquotes
:command:`addquotes: BOOLEAN`
  Parameter mapped to `ADDQUOTES` parameter of Redshift`s `UNLOAD` statement

  * :command:`addquotes: true`

parallel
:command:`parallel: TYPE`
  Parameter mapped to `PARALLEL` parameter of Redshift`s `UNLOAD` statement

  * :command:`parallel: ON`

temp_credentials
:command:`temp_credentials`
  Whether this operator uses temporary security credentials. *Default*: ``true``.
  This operator tries to use temporary security credentials as follows:
    - If `role_arn` is specified, it calls `AssumeRole` action
    - If not, it calls `GetFederationToken` action

  See details about `AssumeRole` and `GetFederationToken` in the documents of AWS Security Token Service.

  So either of `AssumeRole` or `GetFederationToken` action is called to use temporary security credentials by default for secure operation.
  But if this option is disabled, this operator uses credentials as-is set in the secrets insread of temporary security credentials.

  * :command:`temp_credentials: false`

:command:`session_duration INTEGER`
  Session duration of temporary security credentials. *Default*: ``3 hour``.
  This option isn't used when disabling `temp_credentials`

  * :command:`session_duration: 1800`
        

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

:command:`aws.s3.access_key_id, aws.access_key_id`
  The AWS Access Key ID to use when accessing S3.

:command:`aws.s3.secret_access_key, aws.secret_access_key`
  The AWS Secret Access Key to use when accessing S3.

:command:`aws.s3.region, aws.region`
  An optional explicit AWS Region in which to access S3.

:command:`aws.s3.endpoint`
  An optional explicit API endpoint to use when accessing S3. This overrides the `region` secret.

:command:`aws.s3.sse_c_key`
  An optional Customer-Provided Server-Side Encryption (SSE-C) key to use when accessing S3. Must be Base64 encoded.

:command:`aws.s3.sse_c_key_algorithm`
  An optional Customer-Provided Server-Side Encryption (SSE-C) key algorithm to use when accessing S3.

:command:`aws.s3.sse_c_key_md5`
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



emr>: Amazon Elastic Map Reduce
-------------------------------

The **emr>:** operator can be used to run EMR jobs, create clusters and submit steps to existing clusters.

For detailed information about EMR, see the `Amazon Elastic MapReduce Documentation <https://aws.amazon.com/documentation/elastic-mapreduce/>`_.


.. code-block:: yaml

    +emr_job:
      emr>:
      cluster:
        name: my-cluster
        ec2:
          key: my-ec2-key
          master:
            type: m3.2xlarge
          core:
            type: m3.xlarge
            count: 10
        logs: s3://my-bucket/logs/
      staging: s3://my-bucket/staging/
      steps:
        - type: spark
          application: pi.py
        - type: spark-sql
          query: queries/query.sql
          result: s3://my-bucket/results/${session_uuid}/
        - type: script
          script: scripts/hello.sh
          args: [hello, world]

Secrets
~~~~~~~

:command:`aws.emr.access_key_id, aws.access_key_id`
  The AWS Access Key ID to use when submitting EMR jobs.

:command:`aws.emr.secret_access_key, aws.secret_access_key`
  The AWS Secret Access Key to use when submitting EMR jobs.

:command:`aws.emr.role_arn, aws.role_arn`
  The AWS Role to assume when submitting EMR jobs.

Parameters
~~~~~~~~~~

:command:`cluster: STRING | OBJECT`
  Specifies either the ID of an existing cluster to submit steps to or the configuration of a new cluster to create.

  **Using an existing cluster:**

  .. code-block:: yaml

    cluster: j-7KHU3VCWGNAFL

  **Creating a new minimal ephemeral cluster with just one node:**

  .. code-block:: yaml

    cluster:
      ec2:
        key: my-ec2-key
      logs: s3://my-bucket/logs/

  **Creating a customized cluster with several hosts:**

  .. code-block:: yaml

    cluster:
      name: my-cluster
      auto_terminate: false
      release: emr-5.2.0
      applications:
        - hadoop
        - spark
        - hue
        - zookeeper
      ec2:
        key: my-ec2-key
        subnet_id: subnet-83047402b
        master:
          type: m4.2xlarge
        core:
          type: m4.xlarge
          count: 10
          ebs:
            optimized: true
            devices:
              volume_specifiation:
                iops: 10000
                size_in_gb: 1000
                type: gp2
              volumes_per_instance: 6
        task:
          - type: c4.4xlarge
            count: 20
          - type: g2.2xlarge
            count: 6
      logs: s3://my-bucket/logs/
      bootstrap:
        - install_foo.sh
        - name: Install Bar
          path: install_bar.sh
          args: [baz, quux]

:command:`staging: S3_URI`
  A S3 folder to use for staging local files for execution on the EMR cluster. *Note:* the configured AWS credentials must have permission to put and get objects in this folder.

  * :command:`staging: s3://my-bucket/staging/`

:command:`steps: LIST`
  A list of steps to submit to the EMR cluster.

  .. code-block:: yaml

    steps:
      - type: flink
        application: flink/WordCount.jar

      - type: hive
        script: queries/hive-query.q
        vars:
          INPUT: s3://my-bucket/data/
          OUTPUT: s3://my-bucket/output/
        hiveconf:
          hive.support.sql11.reserved.keywords: false

      - type: spark
        application: spark/pi.scala

      - type: spark
        application: s3://my-bucket/spark/hello.py
        args: [foo, bar]

      - type: spark
        application: spark/hello.jar
        class: com.example.Hello
        jars:
          - libhello.jar
          - s3://td-spark/td-spark-assembly-0.1.jar
        conf:
          spark.locality.wait: 5s
          spark.memory.fraction: 0.5
        args: [foo, bar]

      - type: spark-sql
        query: spark/query.sql
        result: s3://my-bucket/results/${session_uuid}/

      - type: script
        script: s3://my-bucket/scripts/hello.sh
        args: [hello, world]

      - type: script
        script: scripts/hello.sh
        args: [world]

      - type: command
        command: echo
        args: [hello, world]

:command:`action_on_failure: TERMINATE_JOB_FLOW | TERMINATE_CLUSTER | CANCEL_AND_WAIT | CONTINUE`
  The action EMR should take in response to a job step failing.

Output parameters
~~~~~~~~~~~~~~~~~

:command:`emr.last_cluster_id`
  The ID of the cluster created. If a pre-existing cluster was used, this parameter will not be set.
