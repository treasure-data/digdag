Release 0.8.1
=============

Digdag is now open-source under Apache License!

General changes
------------------

* Added ``fail>: MESSAGE`` operator.
* Added ``if>: BOOLEAN`` operator.
* Added notification mechanism. Setting ``notification.type`` configuration enables notification. Notification is triggered using new ``notify>: MESSAGE`` operator.
* ``sla:`` parameter supports ``duration: HH:MM:SS`` syntax in addition to ``time: HH:MM:SS`` syntax.
* ``sla:`` parameter supports ``fail: BOOLEAN`` and ``alert: BOOLEAN`` options. Setting ``fail: true`` makes the workflow failed. Setting ``alert: true`` sends an notification using above notification mechanism.
* Fixed an undeterministic error around H2 database that causes "Table not found" exception when a workflow is running for long time.
* Added ``td_for_each>:`` operator.

Client mode changes
-------------------

* Added ``sessions`` and ``session`` commands to show information of sessions. A session is an entity that has one or more attempts.
* ``-r, --revision`` option of ``push`` command is now optional. It generates a random UUID if it's not set.
* ``retry`` command now supports ``--resume`` and ``--resume-from`` options.
* ``--name`` option of ``retry`` command is now optional. It generates a random UUID if it's not set.

Local mode changes
------------------

* Added ``--max-task-threads N`` option to ``run``, ``server``, and ``scheduler`` commands. This option limits number of maximum running tasks.

Server mode changes
-------------------

* Added ``--disable-local-agent`` option.
* Added ``--disable-executor-loop`` option.
* Added session REST API set that is used by the web UI.

Plugin API changes
-------------------

* Added Plugin API that includes ``io.digdag.util.BaseOperator``, ``io.digdag.util.RetryControl``, and ``io.digdag.util.Workspace`` classes.

Release Date
------------
2016-06-14

Contributors
------------------
* Daniel Norberg
* Lewuathe
* Sadayuki Furuhashi

