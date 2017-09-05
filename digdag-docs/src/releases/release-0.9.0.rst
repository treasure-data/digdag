Release 0.9.0
=============

Client mode changes
-------------------

* Added a ``download`` command that can be used to download projects from the server.
* Commands automatically retry HTTP GET requests when one fails with server-side errors.

Server mode changes
-------------------

* The UI is now integrated to the server. Opening the server's HTTP endpoint with a browser shows the UI.

* The UI now visualizes the workflow task execution timeline.

Workflow changes
----------------

* Added an ``emr>`` operator that can be used to run Amazon EMR jobs.

* Added a ``rename_tables`` option to the ``td_ddl>`` operator that can be used to rename TD tables.

* Added a ``skip_on_overtime: true | false`` schedule option that can be used to control whether scheduled session execution should be skipped if another session is already running.

* Scheduled workflow sessions now have a ``last_executed_session_time`` variable which contains the previously executed session time. It is usually same with ``last_session_time`` but has different value when ``skip_on_overtime: true`` is set or the session is the first execution.

General changes
---------------

* **IMPORTANT**: Secrets now use ``_`` (underscore) in names instead of ``-`` (dash). Following secret names are changed:

  * aws.access_key_id, aws.s3.access_key_id
  * aws.secret_access_key, aws.s3.secret_access_key
  * aws.s3.sse_c_key, aws.s3.sse_c_key_algorithm, aws.s3.sse_c_key_md5

* **IMPORTANT**: REST API includes backward incompatible changes.

  * The REST API now wraps collections in JSON objects. This is intended for future improvements to add additional fields without breaking clients again.
  * Type of ID fields are changed from integer to string. This is intended for future improvements to update the encoding of ID fields without breaking clients again.

* **IMPORTANT**: An undocumented functionality in ``sh>``, ``py>``, and ``rb>`` operators includes backward incompatible changes.

  * It affects only if workflow definition includes ``_env`` directive.

Plugin API changes
-------------------

* Operator interface is updated.

Release Date
------------
2016-12-14

Contributors
------------------
* Daniel Norberg
* Daniele Zannotti
* Sadayuki Furuhashi

