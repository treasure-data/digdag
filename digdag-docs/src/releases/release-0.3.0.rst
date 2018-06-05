Release 0.3.0
==================================

Command-line Changes
--------------------

* **IMPORTANT**: arguments of ``run`` command changed:

  * Run command stores session state files at ``digadg.status`` directory by default.

  * Run command uses today's 00:00:00 as the default session_time by default. With this, run command almost always utilize session state files. More likely tasks will be skipped.

  * Run command supports ``--hour`` option in addition to ``-t, session_time`` option for ease of config.

  * Run command supports ``-a, --all``, ``-s, --start``, ``-S, -start-stop``, ``-e, --end`` to control session states.

  * ``-e, --show-params`` is renamed to ``-E, --show-params``.

  * ``-s, --status`` is renamed to ``-o, --save``.

* **IMPORTANT**: store and export are merged instead of replacing. For example, in following case:

  .. code-block:: yaml

    _export:
      mail:
        port: 587
        from: frsyuki@gmail.com
    +workflow:
      _export:
        mail:
          from: sf@treasure-data.com
      +step1:
        sh>: echo ${mail.port}

  Output of +step1 was null because the entire strcture of ``mail`` was replaced. From this version, output will be "587" because parameters are merged.

* td, td_load and td_table_export operators store td.last_job_id parameter.

Release Date
------------------
2016-02-29

Contributors
------------------
* Sadayuki Furuhashi

