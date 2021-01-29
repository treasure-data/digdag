Command reference
==================================

.. contents::
   :local:

Local-mode commands
----------------------------------

init
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag init <dir>

Creates a new workflow project. This command generates a sample .dig file, scripts used in the workflow, and .gitignore file in **<dir>** directory. Examples:

.. code-block:: console

    $ digdag init mydag

Options:

:command:`-t, --type EXAMPLE_TYPE`
  Use specified example project type (default: echo).

  Available types are `echo`, `sh`, `ruby`, `python`, `td` or `postgresql`.

  Example: ``-t sh``


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

:command:`--project DIR`
  Use this directory as the project directory (default: current directory).

  Example: ``--project workflow/``

:command:`-o, --save DIR`
  Use this directory to read and write session status (default: .digdag/status).

  Digdag creates a file in this directory when a task successfully finishes. When digdag runs again, it skips tasks if this a file exists in this directory. This is useful to resume a failed workflow from the middle.

  Example: ``-o .digdag/status``

:command:`-a, --rerun`
  Rerun all tasks even if the tasks successfully finished before. In other words, ignore files at ``-o, --save`` directory.

  Example: ``--rerun``

:command:`-s, --start +NAME`
  If this option is set, Digdag runs this task and following tasks even if the tasks successfully finished before. The other tasks will be skipped if their state files are stored at ``-o, --save`` directory.

  Example: ``--start +step2``

:command:`-g, --goal +NAME`
  If this option is set, Digdag runs this task and its children tasks even if the tasks successfully finished before. The other tasks will be skipped if their state files are stored at ``-o, --save`` directory.

  Example: ``--goal +step2``

:command:`-e, --end +NAME`
  Stops workflow right before this task. This task and following tasks will be skipped.

  Example: ``--end +step4``

:command:`--session EXPR`
  Set session_time to this time. Argument is either of:

    * daily: uses today's 00:00:00 as the session time (update session time every day).
    * hourly: uses current hour's 00:00 as the session time (update session time every hour).
    * schedule: calculates time based on ``schedule`` configuration of the workflow. Error if ``schedule`` is not set.
    * last: reuses the last session time of the last execution. If it's not available, tries to calculate based on ``schedule``, or uses today's 00:00:00.
    * timestamp in *yyyy-MM-dd* or *yyyy-MM-dd HH:mm:ss* format: uses the specified time as the session time.

  Default is "last".

  Example: ``--session 2016-01-01``

:command:`--no-save`
  Disables session state files completely.

  Example: ``--no-save``

:command:`--max-task-threads N`
  Limit maximum number of task execution threads.

  Example: ``--max-task-threads 5``

:command:`-O, --task-log DIR`
  Store task logs to this directory.

  Example: ``--task-log log/tasks``

:command:`-p, --param KEY=VALUE`
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is available using ``${...}`` syntax in the YAML file, or using language API.

  Example: ``-p environment=staging``

  Note: Variable defined in _export is not overwritable by --param option.

:command:`-P, --params-file PATH`
  Read parameters from a YAML/JSON file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: ``-P params.yml``

:command:`-d, --dry-run`
  Dry-run mode. This mode doesn't run tasks.

  Example: ``-d``

:command:`-E, --show-params`
  Show calculated parameters given to a task before running the task. Useful to use with dry-run mode.

  Example: ``-dE``


check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag check [workflow.dig] [options...]

Shows workflow definitions and schedules. "c" is alias of check command. Examples:

.. code-block:: console

    $ digdag c
    $ digdag check
    $ digdag check another.dig

:command:`--project DIR`
  Use this directory as the project directory (default: current directory).

  Example: ``--project workflow/``

:command:`-p, --param KEY=VALUE`
  Overwrite a parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is available using ``${...}`` syntax in the YAML file, or using language API.

  Example: ``-p environment=staging``

  Note: Variable defined in _export is not overwritable by --param option.

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: ``-P params.yml``


scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag scheduler [options...]

Runs a workflow scheduler that runs schedules periodically. This picks up all workflow definition files named with ``.dig`` suffix at the current directory. Examples:

.. code-block:: console

    $ digdag scheduler
    $ digdag scheduler -d status
    $ digdag scheduler -b 0.0.0.0

:command:`--project DIR`
  Use this directory as the project directory (default: current directory).

  Example: ``--project workflow/``

:command:`-n, --port PORT`
  Port number to listen for web interface and api clients (default: 65432).

  Example: ``-p 8080``

:command:`-b, --bind ADDRESS`
  IP address to listen HTTP clients (default: 127.0.0.1).

  Example: ``-b 0.0.0.0``

:command:`-o, --database DIR`
  Store status to this database. Default is memory that doesn't save status.

  Example: ``--database digdag``

:command:`-O, --task-log DIR`
  Store task logs to this directory. If this option is not set, ``digdag log`` command doesn't work.

  Example: ``--task-log digdag.log``

:command:`--max-task-threads N`
  Limit maximum number of task execution threads on this server.

  Example: ``--max-task-threads 5``

:command:`-p, --param KEY=VALUE`
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is available using ``${...}`` syntax in the YAML file, or using language API.

  Example: ``-p environment=staging``

  Note: Variable defined in _export is not overwritable by --param option.

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: ``-P params.yml``

:command:`-c, --config PATH`
  Configuration file to load. (default: ~/.config/digdag/config)

  Example: ``-c digdag-server/server.properties``

selfupdate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag selfupdate [version]

Updates the executable binary file to the latest version or specified version. Examples:

.. code-block:: console

    $ digdag selfupdate
    $ digdag selfupdate 0.10.0

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

  Example: ``-n 8080``

:command:`-b, --bind ADDRESS`
  IP address to listen HTTP clients (default: 127.0.0.1).

  Example: ``-b 0.0.0.0``

:command:`-o, --database DIR`
  Store status to this database.

  Example: ``--database digdag``

:command:`-m, --memory`
  Store status in memory. Data will be removed when the server exits.

  Example: ``--memory``

:command:`-O, --task-log DIR`
  Store task logs to this directory. If this option is not set, ``digdag log`` command doesn't work.

  Example: ``--task-log digdag/sessions``

:command:`-A, --access-log DIR`
  Store access logs to this directory.

  Example: ``--access-log digdag/log``

:command:`--disable-local-agent`
  Disable task execution on this server.

  This option is useful when there're multiple servers sharing the same underlay database and some of the servers are prepared only for REST API. See also ``--disable-executor-loop`` option.

  Example: ``--disable-local-agent``

:command:`--max-task-threads N`
  Limit maximum number of task execution threads on this server.

  Example: ``--max-task-threads 5``

:command:`--disable-executor-loop`
  Disable workflow executor on this server. Workflow executor loop updates state of tasks on the underlay database. At least one server that is sharing the same underlay database must enable workflow executor loop.

  This option is useful when there're multiple servers sharing the same underlay database and some of the servers are prepared only for task execution or REST API. See also ``--disable-local-agent`` option.

  Example: ``--disable-executor-loop``

:command:`--disable-scheduler`
  Disable a schedule executor on this server.

  This option is useful when you want to disable all schedules without modifying workflow files. See also ``--disable-executor-loop`` option.

  Example: ``--disable-scheduler``

:command:`-p, --param KEY=VALUE`
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is available using ``${...}`` syntax in the YAML file, or using language API.

  Example: ``-p environment=staging``

  Note: Variable defined in _export is not overwritable by --param option.

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: ``-P params.yml``

:command:`-c, --config PATH`
  Configuration file to load. (default: ~/.config/digdag/config) See the followings for details.

  Example: ``-c digdag-server/server.properties``


In the config file, following parameters are available

* server.bind (ip address)
* server.port (integer)
* server.admin.bind (ip address)
* server.admin.port (integer)
* server.access-log.path (string. same with --access-log)
* server.access-log.pattern (string, "json", "combined" or "common")
* server.http.io-threads (number of HTTP IO threads in integer. default: available CPU cores or 2, whichever is greater)
* server.http.worker-threads (number of HTTP worker threads in integer. default: server.http.io-threads * 8)
* server.http.no-request-timeout (maximum allowed time for clients to keep a connection open without sending requests or receiving responses in seconds. default: 60)
* server.http.request-parse-timeout (maximum allowed time of reading a HTTP request in seconds. this doesn't affect on reading request body. default: 30)
* server.http.io-idle-timeout (maximum allowed idle time of reading HTTP request and writing HTTP response in seconds. default: 300)
* server.http.enable-http2 (enable HTTP/2. default: false)
* server.http.headers.KEY = VALUE (HTTP header to set on API responses)
* server.jmx.port (port to listen JMX in integer. default: JMX is disabled) Since Java 9, to use this option, you need to set '-Djdk.attach.allowAttachSelf=true' to command line option of java or to JDK_JAVA_OPTIONS environment variable.
* server.authenticator.type (string) The name an authenticator plugin. (See also Authenticator Plugins section bellow): ``basic``
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
* database.maximumPoolSize (integer, default: available CPU cores * 32)
* database.minimumPoolSize (integer, default: same as database.maximumPoolSize)
* database.leakDetectionThreshold (HikariCP leakDetectionThreshold milliseconds in integer. default: 0. To enable, set to >= 2000.)
* database.migrate (enable DB migration. default: true)
* archive.type (type of project archiving, "db", "s3" or "gcs". default: "db")
* archive.s3.endpoint (string. default: "s3.amazonaws.com")
* archive.s3.bucket (string)
* archive.s3.path (string)
* archive.s3.credentials.access-key-id (string. default: instance profile)
* archive.s3.credentials.secret-access-key (string. default: instance profile)
* archive.s3.path-style-access (boolean. default: false)
* archive.gcs.bucket (string)
* archive.gcs.credentials.json.path (string. if not set, auth with local authentication information. Also if path and content are set, path has priority.)
* archive.gcs.credentials.json.content (string. if not set, auth with local authentication information. Also if path and content are set, path has priority.)
* log-server.type (type of log storage, "local" , "null", "s3" or "gcs". default: "null". This parameter will be overwritten with "local" if ``-O, --task-log DIR`` is set.)
* log-server.s3.endpoint (string, default: "s3.amazonaws.com")
* log-server.s3.bucket (string)
* log-server.s3.path (string)
* log-server.s3.direct_download (boolean. default: false)
* log-server.s3.credentials.access-key-id (string. default: instance profile)
* log-server.s3.credentials.secret-access-key (string. default: instance profile)
* log-server.s3.path-style-access (boolean. default: false)
* log-server.gcs.bucket (string)
* log-server.gcs.credentials.json.path (string. if not set, auth with local authentication information. Also if path and content are set, path has priority.)
* log-server.gcs.credentials.json.content (string. if not set, auth with local authentication information. Also if path and content are set, path has priority.)
* log-server.local.path (string. default: digdag.log)
* log-server.local.split_size (long. max log file size in bytes(uncompressed).  default: 0  (not splitted))
* digdag.secret-encryption-key = (base64 encoded 128-bit AES encryption key)
* executor.task_ttl (string. default: 1d. A task is killed if it is running longer than this period.)
* executor.task_max_run (integer. default: 1000. Max number of tasks in workflow.)
* executor.attempt_ttl (string. default: 7d. An attempt is killed if it is running longer than this period.)
* executor.attempt_max_run (integer. default: 100. Max number of running attempts at once per each site_id.)
* executor.enqueue_random_fetch (enqueue ready tasks randomly. default: false)
* executor.enqueue_fetch_size ( Number of tasks to be enqueued. default: 100)
* api.max_attempts_page_size (integer. The max number of rows of attempts in api response)
* api.max_sessions_page_size (integer. The max number of rows of sessions in api response)
* api.max_archive_total_size_limit (integer. The maximum size of an archived project. i.e. ``digdag push`` size. default: 2MB(2\*1024\*1024))
* eval.js-engine-type (type of ConfigEvalEngine. "nashorn" or "graal". "nashorn" is default on Java8 and "graal" is default on Java11)
* eval.extended-syntax (boolean, default: true. Enable or disable extended syntax in graal. If true, nested ``{..}`` is allowed)

Authenticator Plugins
*********************

Authenticator implementation is to be provided by a system plugin (See `System plugins section in Internal architecture <internal.html#system-plugins>`). Interface is ``io.digdag.spi.AuthenticatorFactory``.

**Basic Auth**

Enabled by default (``server.authenticator.type = basic``).

Configuration:

* server.authenticator.basic.username (string, if not set, no authentications happen)
* server.authenticator.basic.password (string. Required if username is set)
* server.authenticator.basic.admin (boolean. default `false`)


Secret Encryption Key
*********************

The secret encryption key is used to encrypt secrets when they are stored in the digdag server database. It must be a valid 128-bit AES key, base64 encoded.

Example:

.. code-block:: none

  digdag.secret-encryption-key = MDEyMzQ1Njc4OTAxMjM0NQ==
  # example
  echo -n '16_bytes_phrase!' | openssl base64
  MTZfYnl0ZXNfcGhyYXNlIQ==

Client-mode commands
----------------------------------

Client-mode common options:

:command:`-e, --endpoint HOST`
  HTTP endpoint of the server (default: http://127.0.0.1:65432)

  Example: ``--endpoint digdag-server.example.com:65432``

:command:`-H, --header KEY=VALUE`
  Add a custom HTTP header. Use multiple times to set multiple headers.

:command:`--basic-auth <user:pass>`
  Add an Authorization header with the provided username and password.

:command:`-c, --config PATH`
  Configuration file to load. (default: ~/.config/digdag/config)

  Example: ``-c digdag-server/client.properties``



You can include following parameters in ~/.config/digdag/config file:

* client.http.endpoint = http://HOST:PORT or https://HOST:PORT
* client.http.headers.KEY = VALUE (set custom HTTP header)
* client.http.disable_direct_download=true (disable direct download in `log` and `download`. effect to server v0.10.0(not yet released) or later.)


start
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag start <project-name> <name> --session <hourly | daily | now | yyyy-MM-dd | "yyyy-MM-dd HH:mm:ss">

Starts a new session. This command requires project name, workflow name, and session_time. Examples:

.. code-block:: console

    $ digdag start myproj main --dry-run --session hourly
    $ digdag start myproj main --session daily
    $ digdag start myproj main --session "2016-01-01 00:00:00"
    $ digdag start myproj main --session "2016-01-01" -p environment=staging -p user=frsyuki

:command:`--session <hourly | daily | now | yyyy-MM-dd | "yyyy-MM-dd HH:mm:ss">`
  Use this time as session_time.

  If ``daily`` is set, today's 00:00:00 is used.

  If ``hourly`` is set, this hour's 00:00 is used.

  If a time is set in "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss" format, this time is used.

  Timezone is based on the workflow's time zone (not your machine's time zone). For example, if a workflow uses Europe/Moscow (+03:00), and your machine's time zone is Asia/Tokyo (+09:00), ``--session 2016-01-01 00:00:00`` means 2016-01-01 00:00:00 +03:00 (2016-01-01 06:00:00 +09:00).

:command:`--retry <name>`
  Set retry attempt name to the new attempt. Usually, you will use ``digdag retry`` command instead of using this option.

:command:`-d, --dry-run`
  Tries to start a new session attempt and validates the results but does nothing.

:command:`-p, --param KEY=VALUE`
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is available using ``${...}`` syntax in the YAML file, or using language API.

  Example: ``-p environment=staging``

  Note: Variable defined in _export is not overwritable by --param option.

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: ``-P params.yml``


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

:command:`--resume`
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

  Example: ``--follow``


kill
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag kill <attempt-id>

Kills a session attempt. Examples:

.. code-block:: console

    $ digdag kill 32


projects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag projects [name]

Shows list of projects or details of a project. Examples:

.. code-block:: console

    $ digdag projects
    $ digdag projects myproj

workflows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag workflows [project-name] [name]

Shows list of workflows or details of a workflow. Examples:

.. code-block:: console

    $ digdag workflows
    $ digdag workflows myproj
    $ digdag workflows myproj main


schedules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag schedules

Shows list of schedules.


disable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag disable [project-name]

Disable all workflow schedules in a project.

.. code-block:: console

    $ digdag disable [schedule-id]
    $ digdag disable [project-name] [name]

Disable a workflow schedule.

.. code-block:: console

    $ digdag disable <schedule-id>
    $ digdag disable myproj
    $ digdag disable myproj main


enable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag enable [project-name]

Enable all workflow schedules in a project.

.. code-block:: console

    $ digdag enable [schedule-id]
    $ digdag enable [project-name] [name]

Enable a workflow schedule.

.. code-block:: console

    $ digdag enable <schedule-id>
    $ digdag enable myproj
    $ digdag enable myproj main


backfill
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag backfill <schedule-id>
    $ digdag backfill <project-name> <name>

Starts sessions of a schedule for past session times.

:command:`-f, --from 'yyyy-MM-dd[ HH:mm:ss]'`
  Timestamp to start backfill from (required). Sessions from this time (including this time) until current time will be started.

  Example: ``--from '2016-01-01'``

:command:`--count N`
  Starts given number of sessions. By default, this command starts all sessions until current time.

  Example: ``--count 5``

:command:`--name NAME`
  Unique name of the new attempts (required). This name is used not to run backfill sessions twice accidentally.

  Example: ``--name backfill1``

:command:`-d, --dry-run`
  Tries to backfill and validates the results but does nothing.


reschedule
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag reschedule <schedule-id>
    $ digdag reschedule <project-name> <name>

Skips a workflow schedule forward to a future time. To run past schedules, use backfill instead.

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

    $ digdag sessions [project-name] [name]

Shows list of sessions. This command shows only the latest attempts of sessions (doesn't include attempts retried by another attempt). To show all attempts, use ``digdag attempts``. Examples:

.. code-block:: console

    $ digdag sessions
    $ digdag sessions myproj
    $ digdag sessions myproj main

:command:`-i, --last-id ID`
  Shows more sessions older than this id.

:command:`-s, --page-size N`
  Shows more sessions of the number of N (in default up to 100).

session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag session [session-id]

Show a single session. Examples:

.. code-block:: console

    $ digdag session <session-id>

attempts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag attempts [session-id]

Shows list of attempts. This command shows all attempts including attempts retried by another attempt. Examples:

.. code-block:: console

    $ digdag attempts
    $ digdag attempts <session-id>

:command:`-i, --last-id ID`
  Shows more attempts older than this id.

:command:`-s, --page-size N`
  Shows more attempts of the number of N (in default up to 100).

attempt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag attempt [attempt-id]

Shows a single attempt. Examples:

.. code-block:: console

    $ digdag attempt <attempt-id>

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

:command:`--project DIR`
  Use this directory as the project directory (default: current directory).

  Example: ``--project workflow/``

:command:`-r, --revision REVISION`
  Unique name of the revision. If this is not set, a random UUID is automatically generated. Typical argument is git's SHA1 hash (``git show --pretty=format:'%T' | head -n 1``) or timestamp (``date +%Y-%m-%dT%H:%M:%S%z``).

  Example: ``-r f40172ebc58f58087b6132085982147efa9e81fb``

:command:`--schedule-from "yyyy-MM-dd HH:mm:ss Z"`
  Start schedules from this time. If this is not set, system time of the server is used. Parameter must include time zone offset. You can run ``date \"+%Y-%m-%d %H:%M:%S %z\"`` command to get current local time.

  Example: ``--schedule-from "2017-07-29 00:00:00 +0200"``

:command:`--copy-outside-symlinks`
  Transform symlinks to regular files or directories if the symlink points a file or directory outside of the target directory. Without this option, such case fails because the files or directories won't be included unless copying.

  Example: ``--copy-outside-symlinks``

download
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag download <project>

Downloads a project archive and extract to a local directory.

.. code-block:: console

    $ digdag download myproj
    $ digdag download myproj -o output
    $ digdag download myproj -r rev20161106

:command:`-o, --output DIR`
  Extract contents to this directory (default: same with project name).

  Example: ``-o output``

:command:`-r, --revision REVISION`
  Download project archive of this revision (default: latest revision).

  Example: ``-r f40172ebc58f58087b6132085982147efa9e81fb``


delete
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag delete <project> [options...]

Deletes a project. Sessions of the deleted project are kept retained so that we can review status of past executions later.

.. code-block:: console

    $ digdag delete myproj

:command:`--force`
  Skip y/N prompt

secrets
~~~~~~~

Digdag provides basic secret management that can be used to securely provide e.g. passwords and api keys etc to operators.

Secrets are handled separately from normal workflow parameters and are stored encrypted by the server. Local secrets are stored in the user home directory.

.. code-block:: console

    $ digdag secrets --project <project>

List secrets set for a project. This will only list the secret keys and will not show the actual secret values.

.. code-block:: console

    $ digdag secrets --project <project> --set key

Set a secret key value for a project. The cli will prompt for the secret value to be entered in the terminal. The entered
value will not be displayed.

Multiple secrets can be entered by listing multiple keys.

It is also possible to read a secret value from a file. Note that the entire raw file contents are read and used as the
secret value. Any whitespace and newlines etc are included as-is.

.. code-block:: console

    $ cat secret.txt
    foobar

    $ digdag secrets --project <project> --set key=@secret.txt

Multiple secrets can be read from a single file in JSON format.

.. code-block:: console

    $ cat secrets.json
    {
        "foo": "secret1",
        "bar": "secret2"
    }

    $ digdag secrets --project <project> --set @secrets.json

Secrets can also be read from stdin. The below command would set the secret key `foo` to the value `bar`.

.. code-block:: console

    $ echo -n 'bar' | digdag secrets --project <project> --set foo=-

Note that only one secret value can be read using the above command. To read multiple secrets from stdin, omit the secret key
name on the command line and provide secret keys and values on stdin in JSON format.

.. code-block:: console

    $ echo -n '{"foo": "secret1", "bar": "secret2"}' | digdag secrets --project <project> --set -

    $ cat secrets.json | digdag secrets --project <project> --set -

To delete secrets, use the `--delete` command.

.. code-block:: console

    $ digdag secrets --project <project> --delete foo bar

Secrets can also be used in local mode. Local secrets are used when running workflows in local mode using `digdag run`.

.. code-block:: console

    $ digdag secrets --local

The above command lists all local secrets.

.. code-block:: console

    $ digdag secrets --local --set foo

The above command sets the local secret `foo`.

.. code-block:: console

    $ digdag secrets --local --delete foo bar

The above command deletes the local secrets `foo` and `bar`.

version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag version

Show client and server version.

Common options
----------------------------------

:command:`-L, --log PATH`
  Output log messages to a file (default is STDOUT). If this option is set, log files are rotated every 10MB, compresses it using gzip, and keeps at most 5 old files.

:command:`-l, --log-level LEVEL`
  Change log level (enum: trace, debug, info, warn, or error. default is info).

:command:`-X KEY=VALUE`
  Add a performance system configuration. This option is for experimental use.

