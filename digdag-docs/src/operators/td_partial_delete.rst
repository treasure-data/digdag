td_partial_delete>: Delete range of Treasure Data table
======================================================

**td_partial_delete>** operator deletes records from a Treasure Data table.

.. warning::

    This operator is deprecated. Use DELETE function in Trino/Presto by td operator instead.

Please be aware that records imported using streaming import can't be deleted for several hours using td_partial_delete. Records imported by INSERT INTO, Data Connector, and bulk imports can be deleted immediately.

Time range needs to be hourly. Setting non-zero values to minutes or seconds will be rejected.

.. code-block:: yaml

    +step1:
      td_partial_delete>: my_table
      database: mydb
      from: 2016-01-01T00:00:00+08:00
      to:   2016-02-01T00:00:00+08:00

Secrets
-------

When you set those parameters, use `digdag secrets command <https://docs.digdag.io/command_reference.html#secrets>`.

- **td.apikey**: API_KEY
  The Treasure Data API key to use when running Treasure Data queries.

Parameters
----------

- **td_partial_delete>**: NAME

  Name of the table.

  Examples:

  .. code-block:: yaml

      td_partial_delete>: my_table

- **database**: NAME

  Name of the database.

  Examples:

  .. code-block:: yaml

      database: my_database

- **from**: yyyy-MM-ddTHH:mm:ss[Z]

  Delete records from this time (inclusive). Actual time range is ``[from, to)``. Value should be a UNIX timestamp integer (seconds) or string in ISO-8601 (yyyy-MM-ddTHH:mm:ss[Z]) format.

  Examples:

  .. code-block:: yaml

      from: 2016-01-01T00:00:00+08:00

- **to**: yyyy-MM-ddTHH:mm:ss[Z]

  Delete records to this time (exclusive). Actual time range is ``[from, to)``. Value should be a UNIX timestamp integer (seconds) or string in ISO-8601 (yyyy-MM-ddTHH:mm:ss[Z]) format.

  Examples:

  .. code-block:: yaml

      to: 2016-02-01T00:00:00+08:00

- **endpoint**: ADDRESS

  API endpoint (default: api.treasuredata.com)

- **use_ssl**: BOOLEAN

  Enable SSL (https) to access to the endpoint (default: true)
