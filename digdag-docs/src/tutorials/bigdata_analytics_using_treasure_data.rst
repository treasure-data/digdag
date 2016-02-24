BigData analytics using Treasure Data
=====================================

.. contents::
   :local:
   :depth: 1

Setting up API key
----------------------------------

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

Using Presto and Hive
--------------------------------------

.. code-block:: yaml

    _export:
      td:
        apikey: YOUR/API_KEY
        database: www_access
        engine: presto  # default query engine is presto

    +step1:
      # this task uses hive
      td>: queries/step1.sql
      engine: hive  # overwrites engine: parameter

    +step2:
      # this task uses presto
      td>: queries/step2.sql
      create_table: mytable_${session_date_compact}

Working with multiple databases
----------------------------------

``_export`` defines the default parameter. Each tasks can overwrite it:

.. code-block:: yaml

    _export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +step1:
      # this task uses mydb
      td>: queries/step1.sql
      database: mydb  # overwrites database setting

    +step2:
      # this task uses www_access
      td>: queries/step2.sql
      create_table: mytable_${session_date_compact}

