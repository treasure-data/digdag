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

Creates a new workflow project. This command generates a sample digdag.yml, executable digdag file, and .gitignore file in **<dir>** directory. Examples:

.. code-block:: console

    $ digdag init mydag


run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag run [+task] [options...]

Runs workflow.

.. code-block:: console

    $ digdag run
    $ digdag run +another
    $ digdag run -f workflow/another.yml --start +step2
    $ digdag run -f workflow/another.yml --start +step2 --end +step4
    $ digdag run -f workflow/another.yml -g +step1 --hour
    $ digdag run -p environment=staging -p user=frsyuki

Options:

:command:`-f, --file PATH.yml`
  Use this file to load tasks (default: digdag.yml).

  Example: -f tasks/another.yml

:command:`-o, --save DIR`
  Use this directory to read and write session status (default: digdag.status).

  Digdag creates a file in this directory when a task successfully finishes. When digdag runs again, it skips tasks if this a file exists in this directory. This is useful to resume a failed workflow from the middle.

  Example: -o digdag.status

:command:`-a, --all`
  Run all tasks even if the tasks successfully finished before. In other words, ignore files at ``-o, --save`` directory.

  Example: --all

:command:`-s, --start +NAME`
  If this option is set, Digdag runs this task and following tasks even if the tasks successfully finished before. The other tasks will be skipped if their state files are stored at ``-o, --save`` directory.

  Example: --start +step2

:command:`-g, --goal +NAME`
  If this option is set, Digdag runs this task and its children tasks even if the tasks successfully finished before. The other tasks will be skipped if their state files are stored at ``-o, --save`` directory.

  Example: --goal +step2

:command:`-e, --end +NAME`
  Stops workflow right before this task. This task and following tasks will be skipped.

  Example: --end +step4

:command:`--hour`
  Digdag uses the latest schedule time as session_time if _schedule option is set at the workflow. Otherwise, Digdag uses today's 00:00:00 as session_time. If this --hour option is set, Digdag uses this hour's 00:00 as session_time.

  Example: --hour

:command:`-t, --session-time TIME`
  Set session_time to this time. Format of TIME needs to be *yyyy-MM-dd* or *yyyy-MM-dd HH:mm:ss*.

  Example: -t 2016-01-01

:command:`--no-save`
  Disables session state files completely.

  Example: --no-save

:command:`-p, --param KEY=VALUE`
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: -P params.yml

:command:`-d, --dry-run`
  Dry-run mode. This mode doesn't run tasks.

  Example: -d

:command:`-E, --show-params`
  Show calculated parameters given to a task before running the task. Useful to use with dry-run mode.

  Example: -dE


check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag check [options...]

Shows workflow definitions and schedules. "c" is alias of check command. Examples:

.. code-block:: console

    $ digdag c
    $ digdag check
    $ digdag check -f workflow/another.yml

:command:`-f, --file PATH`
  Use this file to load tasks (default: digdag.yml).

  Example: -f tasks/another.yml

:command:`-p, --param KEY=VALUE`
  Overwrite a parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: -P params.yml


scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag scheduler [options...]

Runs a workflow scheduler that runs schedules periodically. Examples:

.. code-block:: console

    $ digdag scheduler
    $ digdag scheduler -d status
    $ digdag scheduler -b 0.0.0.0

:command:`-f, --file PATH`
  Use this file to load tasks (default: digdag.yml). This file is reloaded automatically when it's changed.

  Example: -f tasks/another.yml

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
    $ digdag selfupdate 0.3.6

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

  Example: --task-log digdag.log

:command:`-c, --config PATH`
  Server configuration property path. See `Digdag server <digdag_server.html>`_ for details.

  Example: -c digdag.properties


genapikey
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag genapikey

Generates a API key for authentication. Optionally, creates server / client configuration files to use the generated key. Examples:

.. code-block:: console

    $ digdag genapikey
    $ digdag genapikey -o digdag-server

:command:`-o, --output DIR`
  Creates server and client configration files in this directory.

  Example: -o digdag-server



Client-mode commands
----------------------------------

Client-mode common options:

:command:`-e, --endpoint HOST`
  HTTP endpoint of the server (default: 127.0.0.1:65432)

  Example: digdag-server.example.com:65432

:command:`-k, --apikey APIKEY`
  Authentication API key.

  Example: -k "RqveUY_CG84/nGO8OIMlfwQu7Qzb-TRi9zP0Pif63pcHnQWCCNKXr70"

:command:`-c, --config PATH`
  Configuration file path (default: ~/.digdag/client.properties).

  Example: -c digdag-server/client.properties


start
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag start <repo-name> <+name> [--now or "yyyy-MM-dd HH:mm:ss Z"]

Starts a new session. This command requires repository name, workflow name, and session_time. Examples:

.. code-block:: console

    $ digdag start myrepo +main "2016-01-01 00:00:00 -08:00"

:command:`-p, --param KEY=VALUE`
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

:command:`-P, --params-file PATH`
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: -P params.yml

:command:`-R, --retry NAME`
  Set attempt name to retry a session.

  -R 1


log
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag log <session-id> [+task name prefix]

Shows logs of tasks. This command works only if server (or scheduler) runs with ``-O, --task-log`` option.

.. code-block:: console

    $ digdag log 32
    $ digdag log 32 +main
    $ digdag log 32 +main+task1


kill
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag kill <session-id>

Kills a session. Examples:

.. code-block:: console

    $ digdag kill 32


workflows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag workflows [+name]

Shows list of workflows or details of a workflow. Examples:

.. code-block:: console

    $ digdag workflows
    $ digdag workflows -r myrepo
    $ digdag workflows +main

:command:`-r, --repository NAME`
  Repository name.


schedules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag schedules

Shows list of schedules.


sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag sessions [repo-name] [+name]

Shows list of schedules. Examples:

.. code-block:: console

    $ digdag schedules
    $ digdag schedules myrepo
    $ digdag schedules myrepo +main

:command:`-i, --last-id ID`
  Shows more sessions from this id

tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag tasks <session-id>

Shows tasks of a session. Examples:

.. code-block:: console

    $ digdag tasks 32


push
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag push [-f workflow.yml...] <repository> [options...]

Creates a repository archive and upload it to the server. This command reads list of files to add this archive from STDIN. Examples:

.. code-block:: console

    $ git ls-files | digdag push myrepo -r "$(date +%Y-%m-%dT%H:%M:%S%z)"
    $ find . | digdag push default -r "$(git show --pretty=format:'%T' | head -n 1)"

STDIN
  Names of the files to add the archive.

:command:`-f, --file PATH.yml`
  Use this file to load tasks (default: digdag.yml)

  Example: -f tasks/another.yml

:command:`-r, --revision REVISION`
  Name of the revision (required)

  Example: -r 2016-03-02T13:41:39-0800


Common options
----------------------------------

:command:`-L, --log PATH`
  Output log messages to a file (default is STDOUT). If this option is set, log files are rotated every 10MB, compresses it using gzip, and keeps at most 5 old files.

:command:`-l, --log-level LEVEL`
  Change log level (enum: trace, debug, info, warn, or error. default is info).

:command:`-X KEY=VALUE`
  Add a performance system configuration. This option is for experimental use.

