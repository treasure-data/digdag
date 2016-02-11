Task types
==================================

.. contents::
   :local:
   :depth: 2

require>: Runs another workflow
----------------------------------

**require>:** task runs another workflow. It's skipped if the workflow is already done successfully.

Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    run: +main
    +main:
      require>: +another
    +another:
      sh>: tasks/another.sh

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=============================== =========================================== ==========================
Name                            Description                                 Example
=============================== =========================================== ==========================
require>: +NAME                 Name of a workflow                          Task::MyWorkflow.my_task
require: FILE                   Name of a file to require                   task/my_workflow
=============================== =========================================== ==========================

py>: Python scripts
----------------------------------

**py>:** task runs a Python script using ``python`` command.
TODO: link to `Python API documents <ruby_api.html>`_ for details including variable mappings to keyword arguments.

Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    +step1:
      py>: my_step1_method
    +step2:
      py>: tasks.MyWorkflow.step2

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=============================== =========================================== ==========================
Name                            Description                                 Example
=============================== =========================================== ==========================
py>: [PACKAGE.CLASS.]METHOD     Name of a method to run                     tasks.MyWorkflow.my_task
=============================== =========================================== ==========================


rb>: Ruby scripts
----------------------------------

**rb>:** task runs a Ruby script using ``ruby`` command.

TODO: add more description here
TODO: link to `Ruby API documents <python_api.html>`_ for details including best practices how to configure the workflow using ``export: require:``.

Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    export:
      ruby:
        require: tasks/my_workflow

    +step1:
      rb>: my_step1_method
    +step2:
      rb>: Task::MyWorkflow.step2

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=============================== =========================================== ==========================
Name                            Description                                 Example
=============================== =========================================== ==========================
rb>: [MODULE::CLASS.]METHOD     Name of a method to run                     Task::MyWorkflow.my_task
require: FILE                   Name of a file to require                   task/my_workflow
=============================== =========================================== ==========================


sh>: Shell scripts
----------------------------------

**sh>:** task runs a shell script.

TODO: add more description here

Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    +step1:
      sh>: tasks/step1.sh
    +step2:
      sh>: tasks/step2.sh

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=============================== =========================================== ==========================
Name                            Description                                 Example
=============================== =========================================== ==========================
sh>: COMMAND [ARGS...]          Name of the command to run                  tasks/workflow.sh --task1
=============================== =========================================== ==========================


td>: Treasure Data queries
----------------------------------

**td>:** task runs a Hive or Presto query on Treasure Data.

TODO: add more description here

Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    +step2:
      td>: queries/step2.sql
      insert_into: mytable

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=============================== =========================================== ==========================
Name                            Description                                 Example
=============================== =========================================== ==========================
td>: FILE.sql                   Path to a query template file               queries/step1.sql
create_table: NAME              Name of a table to create from the results  my_table
insert_into: NAME               Name of a table to append results into      my_table
database: NAME                  Name of a database                          my_db
apikey: APIKEY                  API key                                     992314/abcdef0123456789abcdef0123456789
engine: presto                  Query engine (``presto`` or ``hive``)       hive
=============================== =========================================== ==========================


td_ddl>: Treasure Data operations
----------------------------------

**type: td_ddl** task runs an operational task on Treasure Data.

TODO: add more description here

Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=============================== ================================================= ==========================
Name                            Description                                       Example
=============================== ================================================= ==========================
td>: FILE.sql                   Path to a query template file                     queries/step1.sql
create_table: NAME              Create a new table if not exists                  my_table
empty_table: NAME               Create a new table (drop it first if it exists)   my_table
drop_table: NAME                Drop a table if exists                            my_table
apikey: APIKEY                  API key                                           992314/abcdef0123456789abcdef0123456789
=============================== ================================================= ==========================

mail>: Sending email
----------------------------------

**mail>:** task sends an email.

To use Gmail SMTP server, you need to do either of:

  a) Generate a new app password at `App passwords <https://security.google.com/settings/security/apppasswords>`_. This needs to enable 2-Step Verification first.

  b) Enable access for less secure apps at `Less secure apps <https://www.google.com/settings/security/lesssecureapps>`_. This works even if 2-Step Verification is not enabled.

Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=============================== ================================================= ==========================
Name                            Description                                       Example
=============================== ================================================= ==========================
mail>: SUBJECT                  Subject of the email                              Mail From Digdag
body: TEXT                      Email body                                        Hello, this is from Digdag
to: [ADDR1, ADDR2, ...]         To addresses                                      analyst@examile.com
from: ADDR                      From address                                      admin@example.com
host: NAME                      SMTP host name                                    smtp.gmail.com
port: NAME                      SMTP port number                                  587
username: NAME                  SMTP login username if authentication is required me
password: APIKEY                SMTP login password                               MyPaSsWoRd
tls: BOOLEAN                    Enables TLS handshake                             true
ssl: BOOLEAN                    Enables legacy SSL encryption                     false
debug: BOOLEAN                  Shows debug logs                                  false
=============================== ================================================= ==========================


embulk>: Embulk data transfer
----------------------------------

**embulk>:** task runs `Embulk `http://www.embulk.org>`_ to transfer data across storages including local files.

Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=============================== ================================================= ==========================
Name                            Description                                       Example
=============================== ================================================= ==========================
embulk>: FILE.yml               Path to a configuration template file             embulk/mysql_to_csv.yml
=============================== ================================================= ==========================

