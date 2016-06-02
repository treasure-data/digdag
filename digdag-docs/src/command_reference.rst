Command reference
==================================

.. contents::
   :local:

Local-mode commands
----------------------------------

new
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag new <dir>

Creates a new workflow project. This command generates a sample digdag.dig, executable digdag file, and .gitignore file in **<dir>** directory. Examples:

.. code-block:: console

    $ digdag init mydag


run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag run <workflow.dig> [+task] [options...]

Runs workflow.

.. code-block:: console

    $ digdag run workflow.dig
    $ digdag run workflow.dig +step2
    $ digdag run another.dig --start +step2
    $ digdag run another.dig --start +step2 --end +step4
    $ digdag run another.dig -g +step1 --hour
    $ digdag run workflow.dig -p environment=staging -p user=frsyuki
    $ digdag run workflow.dig --session hourly

Options:

:command:`---project DIR`
  Use this directory as the project directory (default: current directory).

  Example: --project workflow/

:command:`-o, --save DIR`
  Use this directory to read and write session status (default: .digdag/status).

  Digdag creates a file in this directory when a task successfully finishes. When digdag runs again, it skips tasks if this a file exists in this directory. This is useful to resume a failed workflow from the middle.

  Example: -o .digdag/status

:command:`-a, --rerun`
  Rerun all tasks even if the tasks successfully finished before. In other words, ignore files at ``-o, --save`` directory.

  Example: --rerun

:command:`-s, --start +NAME`
  If this option is set, Digdag runs this task and following tasks even if the tasks successfully finished before. The other tasks will be skipped if their state files are stored at ``-o, --save`` directory.

  Example: --start +step2

:command:`-g, --goal +NAME`
  If this option is set, Digdag runs this task and its children tasks even if the tasks successfully finished before. The other tasks will be skipped if their state files are stored at ``-o, --save`` directory.

  Example: --goal +step2

:command:`-e, --end +NAME`
  Stops workflow right before this task. This task and following tasks will be skipped.

  Example: --end +step4

:command:`--session EXPR`
  Set session_time to this time. Argument is either of:

    * daily: uses today's 00:00:00 as the session time (update session time every day).
    * hourly: uses current hour's 00:00 as the session time (update session time every hour).
    * schedule: calculates time based on ``schedule`` configuration of the workflow. Error if ``schedule`` is not set.
    * last: reuses the last session time of the last execution. If it's not available, tries to calculate based on ``schedule``, or uses today's 00:00:00.
    * timestmap in *yyyy-MM-dd* or *yyyy-MM-dd HH:mm:ss* format: uses the specified time as the session time.

  Default is "last".

  Example: --session 2016-01-01

:command:`--no-save`
  Disables session state files completely.

  Example: --no-save

:command:`-p, --param KEY=VALUE`
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: -P params.dig

:command:`-d, --dry-run`
  Dry-run mode. This mode doesn't run tasks.

  Example: -d

:command:`-E, --show-params`
  Show calculated parameters given to a task before running the task. Useful to use with dry-run mode.

  Example: -dE


check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag check [workflow.dig] [options...]

Shows workflow definitions and schedules. "c" is alias of check command. Examples:

.. code-block:: console

    $ digdag c
    $ digdag check
    $ digdag check another.dig

:command:`---project DIR`
  Use this directory as the project directory (default: current directory).

  Example: --project workflow/

:command:`-p, --param KEY=VALUE`
  Overwrite a parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: -P params.dig


scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag scheduler [options...]

Runs a workflow scheduler that runs schedules periodically. This picks up all workflow definition files named with ``.dig`` suffix at the current directory. Examples:

.. code-block:: console

    $ digdag scheduler
    $ digdag scheduler -d status
    $ digdag scheduler -b 0.0.0.0

:command:`---project DIR`
  Use this directory as the project directory (default: current directory).

  Example: --project workflow/

:command:`-n, --port PORT`
  Port number to listen for web interface and api clients (default: 65432).

  Example: -p 8080

:command:`-b, --bind ADDRESS`
  IP address to listen HTTP clients (default: 127.0.0.1).

  Example: -b 0.0.0.0

:command:`-o, --database DIR`
  Store status to this database. Default is memory that doesn't save status.

  Example: --database digdag

:command:`-O, --task-log DIR`
  Store task logs to this directory. If this option is not set, ``digdag log`` command doesn't work.

  Example: --task-log digdag.log

:command:`--max-task-threads N`
  Limit maxium number of task execution threads on this server.

  Example: --max-task-threads 5

:command:`-p, --param KEY=VALUE`
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: -P params.dig

:command:`-c, --config PATH`
  Server configuration property path. This is same with server command. See `Digdag server <digdag_server.html>`_ for details.

  Example: -c digdag.properties


selfupdate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag selfupdate [version]

Updates the executable binary file to the latest version or specified version. Examples:

.. code-block:: console

    $ digdag selfupdate
    $ digdag selfupdate 0.8.0

Server-mode commands
----------------------------------

server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag server [options...]

Runs a digdag server. --memory or --database option is required. Examples:

.. code-block:: console

    $ digdag server --memory
    $ digdag server -o digdag-server
    $ digdag server -o digdag-server -b 0.0.0.0

:command:`-n, --port PORT`
  Port number to listen for web interface and api clients (default: 65432).

  Example: -p 8080

:command:`-b, --bind ADDRESS`
  IP address to listen HTTP clients (default: 127.0.0.1).

  Example: -b 0.0.0.0

:command:`-o, --database DIR`
  Store status to this database.

  Example: --database digdag

:command:`-m, --memory`
  Store status in memory. Data will be removed when the server exists.

  Example: --memory

:command:`-O, --task-log DIR`
  Store task logs to this directory. If this option is not set, ``digdag log`` command doesn't work.

  Example: --task-log digdag/sessions

:command:`-A, --access-log DIR`
  Store access logs to this directory.

  Example: --access-log digdag/log

:command:`--disable-local-agent`
  Disable task execution on this server.

  This option is useful when there're multiple servers sharing the same underlay database and some of the servers are prepared only for REST API. See also ``--disable-executor-loop`` option.

  Example: --disable-local-agent

:command:`--max-task-threads N`
  Limit maxium number of task execution threads on this server.

  Example: --max-task-threads 5

:command:`--disable-executor-loop`
  Disable workflow executor on this server. Workflow executor loop updates state of tasks on the underlay database. At least one server that is sharing the same underlay database must enable workflow executor loop.

  This option is useful when there're multiple servers sharing the same underlay database and some of the servers are prepared only for task execution or REST API. See also ``--disable-local-agent`` option.

  Example: --max-task-threads 5

:command:`-c, --config PATH`
  Server configuration property path. See `Digdag server <digdag_server.html>`_ for details.

  Example: -c digdag.properties


In the config file, following parameters are available

* server.bind (ip address)
* server.port (integer)
* server.access-log.path (string. same with --access-log)
* server.access-log.pattern (string, "json", "combined" or "common")
* server.http.headers.KEY = VALUE (HTTP header to set on API responses)
* database.type (enum, "h2" or "postgresql")
* database.user (string)
* database.password (string)
* database.host (string)
* database.port (integer)
* database.database (string)
* database.loginTimeout (seconds in integer, default: 30)
* database.socketTimeout (seconds in integer, default: 1800)
* database.ssl (boolean, default: false)
* database.connectionTimeout (seconds in integer, default: 30)
* database.idleTimeout (seconds in integer, default: 600)
* database.validationTimeout (seconds in integer, default: 5)
* database.maximumPoolSize (integer, default: 10)


Client-mode commands
----------------------------------

Client-mode common options:

:command:`-e, --endpoint HOST`
  HTTP endpoint of the server (default: http://127.0.0.1:65432)

  Example: digdag-server.example.com:65432

:command:`-H, --header KEY=VALUE`
  Add a custom HTTP header. Use multiple times to set multiple headers.

:command:`-c, --config PATH`
  Configuration file to load. (default: ~/.config/digdag/config)

  Example: -c digdag-server/client.properties

You can include following parameters in ~/.config/digdag/config file:

* cilent.http.endpoint = http://HOST:PORT or https://HOST:PORT
* client.http.headers.KEY = VALUE (set custom HTTP header)


start
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag start <project-name> <+name> --session <hourly | daily | now | "yyyy-MM-dd[ HH:mm:ss]">

Starts a new session. This command requires project name, workflow name, and session_time. Examples:

.. code-block:: console

    $ digdag start myproj +main --session daily
    $ digdag start myproj +main --session hourly
    $ digdag start myproj +main --session "2016-01-01 00:00:00"

:command:`--session <hourly | daily | now | "yyyy-MM-dd[ HH:mm:ss]">`
  Use this time as session_time.

  If ``daily`` is set, today's 00:00:00 is used.

  If ``hourly`` is set, this hour's 00:00:00 is used.

  If a time is set in "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss" format, this time is used.

  Timezone is based on the workflow's time zone (not your machine's time zone). For example, if a workflow uses Europe/Moscow (+03:00), and your machine's time zone is Asia/Tokyo (+09:00), ``--session 2016-01-01 00:00:00`` means 2016-01-01 00:00:00 +03:00 (2016-01-01 06:00:00 +09:00).

:command:`--retry <name>`
  Set retry attempt name to the new attempt. Usually, you will use ``digdag retry`` command instead of using this option.

:command:`-d, --dry-run`
  Tries to start a new session attempt and validates the results but does nothing.

:command:`-p, --param KEY=VALUE`
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: -P params.dig


retry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag retry <attempt-id>

Retry a session. One of revision options (``--latest-revision``, ``--keep-revision``, or ``--revision <name>``) and one of resume options (``--all``, ``--resume``, or ``--resume-from <+name>``) are required.

Examples:

.. code-block:: console

    $ digdag retry 35 --latest-revision --all
    $ digdag retry 35 --latest-revision --resume
    $ digdag retry 35 --latest-revision --resume-from +step2
    $ digdag retry 35 --keep-revision --resume
    $ digdag retry 35 --revision rev29a87a9c --resume

:command:`--latest-revision`
  Use the latest revision to retry the session.

:command:`--keep-revision`
  Use the same revision with the specified attempt to retry the session.

:command:`--revision <name>`
  Use a specific revision to retry the session.

:command:`--all`
  Retries all tasks.

:command:`--resume +NAME`
  Retry only failed tasks. Successfully finished tasks are skipped.

:command:`--resume-from +NAME`
  Retry from this task. This task and all following tasks will be executed. All tasks before this task must have been successfully finished.

:command:`--name <name>`
  An unique identifier of this retry attempt. If another attempt with the same name already exists within the same session, request fails with 409 Conflict.


log
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag log <attempt-id> [+task name prefix]

Shows logs of a session attempt. This command works only if server (or scheduler) runs with ``-O, --task-log`` option.

.. code-block:: console

    $ digdag log 32
    $ digdag log 32 -f
    $ digdag log 32 +main
    $ digdag log 32 +main+task1

:command:`-v, --verbose`
  Show all logs. By default, log level less than INFO and lines following those lines are skipped.

:command:`-f, --follow`
  Show new logs until attempt or task finishes. This is similar to UNIX ``tail -f`` command. Because server buffers logs, there're some delay until logs are actually show.

  Example: --follow


kill
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag kill <attempt-id>

Kills a session attempt. Examples:

.. code-block:: console

    $ digdag kill 32


workflows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag workflows [project-name] [+name]

Shows list of workflows or details of a workflow. Examples:

.. code-block:: console

    $ digdag workflows
    $ digdag workflows myproj
    $ digdag workflows +main
    $ digdag workflows myproj +main


schedules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag schedules

Shows list of schedules.


backfill
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag backfill <schedule-id>

Starts sessions of a schedule for past session times.

:command:`-f, --from 'yyyy-MM-dd HH:mm:ss Z'`
  Timestamp to start backfill from (required). Sessions from this time (including this time) until current time will be started.

  Example: --from '2016-01-01 00:00:00 -0800'

:command:`--attempt-name NAME`
  Unique retry attempt name of the new attempts (required). This name is used not to run backfill sessions twice accidentally.

  Example: --attempt-name backfill1

:command:`-d, --dry-run`
  Tries to backfill and validates the results but does nothing.


reschedule
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag reschedule <schedule-id>

Skips schedule forward to a future time. To run past schedules, use backfill instead.

:command:`-s, --skip N`
  Skips specified number of schedules from now. This number "N" doesn't mean number of sessions to be skipped. "N" is the number of sessions to be skipped.

:command:`-t, --skip-to 'yyyy-MM-dd HH:mm:ss Z'`
  Skips schedules until the specified time (exclusive).

:command:`-a, --run-at 'yyyy-MM-dd HH:mm:ss Z'`
  Set next run time to this time.

:command:`-d, --dry-run`
  Tries to reschedule and validates the results but does nothing.


sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag sessions [project-name] [+name]

Shows list of sessions. This command shows only the latest attempts of sessions (doesn't include attempts retried by another attempt). To show all attempts, use ``digdag attempts``. Examples:

.. code-block:: console

    $ digdag sessions
    $ digdag sessions myproj
    $ digdag sessions myproj +main

:command:`-i, --last-id ID`
  Shows more sessions older than this id.


attempts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag attempts [project-name] [+name]

Shows list of attempts. This command shows shows all attempts including attempts retried by another attempt. Examples:

.. code-block:: console

    $ digdag attempts
    $ digdag attempts myproj
    $ digdag attempts myproj +main

:command:`-i, --last-id ID`
  Shows more attempts older than this id.


tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag tasks <attempt-id>

Shows tasks of an session attempt. Examples:

.. code-block:: console

    $ digdag tasks 32


push
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag push <project> [options...]

Creates a project archive and upload it to the server. This command uploads workflow definition files (files with .dig suffix) at the current directory, and all other files from the current directory recursively. Examples:

.. code-block:: console

    $ digdag push myproj -r "$(date +%Y-%m-%dT%H:%M:%S%z)"
    $ digdag push default -r "$(git show --pretty=format:'%T' | head -n 1)"

:command:`---project DIR`
  Use this directory as the project directory (default: current directory).

  Example: --project workflow/

:command:`-r, --revision REVISION`
  Unique name of the revision. If this is not set, a random UUID is automatically generated. Typical argument is git's SHA1 hash (``git show --pretty=format:'%T' | head -n 1``) or timestamp (``date +%Y-%m-%dT%H:%M:%S%z``).

  Example: -r f40172ebc58f58087b6132085982147efa9e81fb


delete
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag delete <project> [options...]

Deletes a project. Sessions of the deleted project are kept retained so that we can review status of past executions later.

.. code-block:: console

    $ digdag delete myproj


Common options
----------------------------------

:command:`-L, --log PATH`
  Output log messages to a file (default is STDOUT). If this option is set, log files are rotated every 10MB, compresses it using gzip, and keeps at most 5 old files.

:command:`-l, --log-level LEVEL`
  Change log level (enum: trace, debug, info, warn, or error. default is info).

:command:`-X KEY=VALUE`
  Add a performance system configuration. This option is for experimental use.

