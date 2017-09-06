Release 0.2.6
==================================

General Changes
------------------

* Added experimental support for per-task logging.

  * If server or scheduler runs with ``-O, --task-log DIR`` option, it stores task logs to the directory.

  * ``digdag log <session-id> [+task name prefix]`` command shows logs of tasks. This command works even if the server runs on a remote machine (``-e, --endpoint`` option is required in this case).

  * This is useful when you run digdag server on a remote machine.


Release Date
------------------
2016-02-25

Contributors
------------------
* Sadayuki Furuhashi

