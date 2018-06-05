Release 0.4.0
==================================

Local-line changes
------------------

* **IMPORTANT**: ``run`` command uses session_time of the last execution as the default session_time (previous default value was today's 00:00:00).

* Added new ``--session`` argument. This accepts ``daily``, ``hourly``, ``schedule``, or timestamp in ``yyyy-MM-dd[ HH:mm:ss]`` format.

* ``-t`` and ``--session-time`` arguments are deprecated.


Server-mode changes
-------------------

* Added support for PostgreSQL database backend. Tested with PostgreSQL >= 9.4. PostgreSQL >= 9.5 is preferred (because SELECT ... FOR UPDATE SKIP RECORD statement is available).

* Database parameters such as maximumPoolSize, loginTimeout, socketTimeout are now configurable.


General Changes
------------------

* Added validation to task names. Now task name can't include some symbols. Allowed characters are ``[a-zA-Z_0-9]`` and ``- = [ ] { } @ , .``.

* Added validation to repository name, revision name, and retry attempt name are strictly validated.

* Adding workflows to existent revision is now prohibited (server mode only).


Release Date
------------------
2016-03-16

Contributors
------------------
* Sadayuki Furuhashi

