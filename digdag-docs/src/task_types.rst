Task types
==================================

.. contents::
   :local:
   :depth: 2

require>: Runs another workflow
----------------------------------

**require>:** task runs another workflow. It's skipped if the workflow is already done successfully.

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

**py>:** task runs a Python script using ``python`` command.
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

**rb>:** task runs a Ruby script using ``ruby`` command.

TODO: add more description here
TODO: link to `Ruby API documents <python_api.html>`_ for details including best practices how to configure the workflow using ``export: require:``.

.. code-block:: yaml

    export:
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

**sh>:** task runs a shell script.

TODO: add more description here

.. code-block:: yaml

    +step1:
      sh>: tasks/step1.sh
    +step2:
      sh>: tasks/step2.sh

:command:`sh>: COMMAND [ARGS...]`
  Name of the command to run.

  * :command:`sh>: tasks/workflow.sh --task1`


td>: Treasure Data queries
----------------------------------

**td>:** task runs a Hive or Presto query on Treasure Data.

TODO: add more description here

.. code-block:: yaml

    export:
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

:command:`td>: FILE.sql`
  Path to a query template file.

  * :command:`td>: queries/step1.sql`

:command:`create_table: NAME`
  Name of a table to create from the results. This option deletes the table if it already exists.

  * :command:`create_table: my_table`

:command:`insert_into: NAME`
  Name of a table to append results into.

  * :command:`insert_into: my_table`

:command:`result_url: NAME`
  Output the query results to the URL:

  * :command:`result_url: tableau://username:password@my.tableauserver.com/?mode=replace`

:command:`database: NAME`
  Name of a database.

  * :command:`database: my_db`

:command:`apikey: APIKEY`
  API key.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

:command:`engine: presto`
  Query engine (``presto`` or ``hive``).

  * :command:`engine: hive`
  * :command:`engine: presto`


td_ddl>: Treasure Data operations
----------------------------------

**type: td_ddl** task runs an operational task on Treasure Data.

TODO: add more description here

.. code-block:: yaml

    export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +step1:
      type: td_ddl
      create_table: my_table_${session_date_compact}
    +step2:
      type: td_ddl
      drop_table: my_table_${session_date_compact}
    +step2:
      type: td_ddl
      empty_table: my_table_${session_date_compact}

:command:`create_table: NAME`
  Create a new table if not exists.

  * :command:`create_table: my_table`

:command:`empty_table: NAME`
  Create a new table (drop it first if it exists).

  * :command:`empty_table: my_table`

:command:`drop_table: NAME`
  Drop a table if exists.

  * :command:`drop_table: my_table`

:command:`apikey: APIKEY`
  API key.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`


mail>: Sending email
----------------------------------

**mail>:** task sends an email.

To use Gmail SMTP server, you need to do either of:

  a) Generate a new app password at `App passwords <https://security.google.com/settings/security/apppasswords>`_. This needs to enable 2-Step Verification first.

  b) Enable access for less secure apps at `Less secure apps <https://www.google.com/settings/security/lesssecureapps>`_. This works even if 2-Step Verification is not enabled.

.. code-block:: yaml

    export:
      mail:
        host: smtp.gmail.com
        port: 587
        from: "you@gmail.com"
        username: "you@gmail.com"
        password: "...password..."
        debug: true

    +step1:
      mail>: this workflow started
      body: Hello
      to: [me@example.com]
    +step2:
      sh>: this_task_might_fail.sh
      error:
        mail>: a task failed
        to: [me@example.com]

:command:`mail>: SUBJECT`
  Subject of the email.

  * :command:`mail>: Mail From Digdag`

:command:`body: TEXT`
  Email body.

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

:command:`password: APIKEY`
  SMTP login password.

  * :command:`password: MyPaSsWoRd`

:command:`tls: BOOLEAN`
  Enables TLS handshake.

  * :command:`tls: true`

:command:`ssl: BOOLEAN`
  Enables legacy SSL encryption.

  * :command:`ssl: false`

:command:`debug: BOOLEAN`
  Shows debug logs (default: false).

  * :command:`debug: false`


embulk>: Embulk data transfer
----------------------------------

**embulk>:** task runs `Embulk <http://www.embulk.org>`_ to transfer data across storages including local files.

.. code-block:: yaml

    +load:
      embulk>: data/load.yml

:command:`embulk>: FILE.yml`
  Path to a configuration template file.

  * :command:`embulk>: embulk/mysql_to_csv.yml`

