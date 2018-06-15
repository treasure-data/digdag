Release 0.5.8
==================================

Server-mode changes
-------------------

* Log REST API endpoints return fileSize field in addition to fileName.

* Optimized out some seconds of delay until a new session starts after submission.


Client-mode changes
-------------------

* ``digdag log`` filters out DEBUG logs, TRACE logs, and lines following those logs by default.

* Added ``digdag log -v`` option to show all logs.

* Added ``digdag log -f`` option that shows new log lines automatically until the session or task finishes.


Release Date
------------------
2016-04-06

Contributors
------------------
* Sadayuki Furuhashi

