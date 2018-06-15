Release 0.6.1
==================================

CLI changes
------------------

* All commands support ``-c, --config PATH`` argument. Default is ``~/.digdag/config``.

* Client-mode commands (``start``, ``push``, etc.) no longer read ``~/.digdag/client.properties`` file.

* ``run``, ``push``, and ``check`` commands use ``params.KEY = VALUE`` parameters written in ``~/.digdag/config`` file. This is useful to set default parameters such as hostname, username, and password of a remote service.

* ``run`` command stores status files in ``.digdag/status`` instead of ``digdag.status``.

* Command exited with a error no longer shows stacktrace.

* ``header.KEY = VALUE`` parameters are changed to ``client.http.headers.KEY = VALUE``.

* ``endpoint = HOST:PORT`` parameter is changed to ``client.http.endpoint = http[s]://HOST:PORT``.

* Client-mode commands support SSL (https).

* Parameters on task groups are validated. If a task group includes an unknown key (such as ``parallel``), digdag shows a suggestion (such as ``did mean [_parallel]?``) and exits.

* JVM version check runs only with ``run``, ``scheduler`` and ``server`` commands. Other commands such as ``push`` and ``workflows`` can use older version of JDK 8.

* ``_retry: N`` option on a task group works.

* ``digdag`` without command name no longer runs ``run`` command implicitly.

* Built-in authentication mechanism is removed. ``genapikey`` command is removed. ``-a, --apikey`` option is removed.

Server mode changes
-------------------

* Added access logging. Disabled by default. ``-A, --access-log DIR`` parameter enables it.


Release Date
------------------
2016-04-22

Contributors
------------------
* Daniel Norberg
* Sadayuki Furuhashi

