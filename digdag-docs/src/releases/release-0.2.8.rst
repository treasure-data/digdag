Release 0.2.8
==================================

Command-line Changes
------------------

* Run command stores sessions state files at ``digadg.status`` directory by default.

* Run command uses today's 00:00:00 as the default session_time by default. With this, run command almost always utilize session state files. More likely tasks will be skipped.

* Run command supports ``-a, --all``, ``-s, --start``, ``-S, -start-stop``, ``-e, --end`` to control session states.

* Run command supports ``--hour`` option in addition to ``-t, session_time`` option for ease of config.

* ``-e, --show-params`` is renamed to ``-E, --show-params``.

* ``-s, --status`` is renamed to ``-o, --save``.

General Changes
------------------

* Added ``loop`` operator. To support it, ``${...}`` varaibles in ``_do`` key will not be evaluated.

Release Date
------------------
2016-02-29
