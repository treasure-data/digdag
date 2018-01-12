Release 0.8.6
=============

Client mode changes
-------------------

* When a new workflow is pushed with a schedule option, the first execution time will be calculated based on the execution time instead of session_time. This solves issue #149.

* Added ``--schedule-from`` option to ``push`` command to control the first scheduled execution time.

* Fixed web UI not to be able to show task execution logs.

* HTTP contents and headers are hidden to not show security information when ``-l debug`` is set.


General changes
---------------

* Added ``queue.db.max_concurrency`` option that limits number of concurrently running tasks per site across all servers.


Release Date
------------
2016-08-04

Contributors
------------------
* Daniel Norberg
* Sadayuki Furuhashi
* Toru Takahashi
* ariarijp

