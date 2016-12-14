Release 0.9.0
=============

Client mode changes
-------------------
* Added a ``download`` command that can be used to download projects from the server.

Server mode changes
-------------------
* The UI now visualizes the workflow task execution timeline.
* The REST API now wraps collections in JSON objects.

Workflow changes
----------------
* Added an ``emr>`` operator that can be used to run AWS EMR jobs.
* Added a ``rename_tables`` option to the ``td_ddl>`` operator that can be used to rename TD tables.
* Added a ``skip_on_overtime: true | false`` schedule option that can be used to control whether scheduled session execution should be skipped if another session is already running.
* Scheduled workflow sessions now have a ``last_executed_session_time`` variable which contains the previously executed session time.

General changes
---------------
* Secrets now use ``_`` (underscore) in names instead of ``-`` (dash).

Release Date
------------
2016-12-14
