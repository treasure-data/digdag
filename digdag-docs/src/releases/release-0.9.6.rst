Release 0.9.6
=============

General Changes
---------------

* ``require>`` operator is fixed so that it fails when dependent workflow fails.
* Added ``ignore_failure: BOOLEAN`` option to ``require>`` operator. If it is set to true, ``require>`` operator ignores failure of dependent workflow.
* Added ``result_connection: NAME`` and ``result_settings: {...}`` options to ``td>`` operator.
* Fixed unexpected duplicated (retried) execution of tasks when ``--max-task-threads`` is set and ``_parallel:`` is used. See `issue #487 <https://github.com/treasure-data/digdag/issues/487>`_ for the detailed condition to reproduce.

Server Changes
---------------

* Added ``userInfo`` field to ``/api/projects/{id}/revisions``.
* Added ``index`` field to ``/api/sessions/{id}/attempts``. Index of an attempt is a sequence number (1, 2, 3, ...) in a session. Intention of this change is to obsolete attempt id to avoid confusion from session id.
* Starting server on admin port is now optional. ``--admin-port`` argument is required to enable admin API.
* Fixed hard timeout of long-running tasks (1 day). It was killing an attempt if the attempt is running longer than 1 day. Now it kills if a single task is running longer than 1 day. Attempt is killed if it is running longer than 7 days (``executor.attempt_ttl`` and ``executor.task_ttl`` are the configuration parameters of the durations).

Release Date
------------
2017-03-14

Contributors
------------------
* Sadayuki Furuhashi
* Toru Takahashi
* szyn

