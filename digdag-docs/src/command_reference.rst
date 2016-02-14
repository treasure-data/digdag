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
    $ digdag run +another -s digdag.status
    $ digdag run -f workflow/another.yml -t "2016-01-01 00:00:00"
    $ digdag run -p environment=staging -p user=frsyuki

Options:

-f, --file PATH
  Use this file to load tasks (default: digdag.yml).

  Example: -f tasks/another.yml

-s, --status DIR
  Use this directory to read and write session status. Digdag creates a file in this directory when a task successfully finishes. When digdag runs again, it skips tasks if this a file exists in this directory. This is useful to resume a failed workflow from the middle.

  Example: -s digdag.status

-t, --session-time TIME
  Set session_time to this time. Format of TIME needs to be *yyyy-MM-dd* or *yyyy-MM-dd HH:mm:ss*.

  Example: -t 2016-01-01

-p, --param KEY_VALUE
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

-P, --params-file PATH
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: -P params.yml


check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag check [options...]

Shows workflow definitions and schedules. "c" is alias of check command. Examples:

.. code-block:: console

    $ digdag c
    $ digdag check
    $ digdag check -f workflow/another.yml

-f, --file PATH
  Use this file to load tasks (default: digdag.yml).

  Example: -f tasks/another.yml

-p, --param KEY_VALUE
  Overwrite a parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

-P, --params-file PATH
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

-f, --file PATH
  Use this file to load tasks (default: digdag.yml). This file is reloaded automatically when it's changed.

  Example: -f tasks/another.yml

-n, --port PORT
  Port number to listen for web interface and api clients (default: 65432).

  Example: -p 8080

-b, --bind ADDRESS
  IP address to listen HTTP clients (default: 127.0.0.1).

  Example: -b 0.0.0.0

-o, --database DIR
  Store status to this database. Default is memory that doesn't save status.

  Example: --database digdag

-c, --config PATH
  Server configuration property path. This is same with server command. See `Digdag server <digdag_server.html>`_ for details.

  Example: -c digdag.properties


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

-n, --port PORT
  Port number to listen for web interface and api clients (default: 65432).

  Example: -p 8080

-b, --bind ADDRESS
  IP address to listen HTTP clients (default: 127.0.0.1).

  Example: -b 0.0.0.0

-o, --database DIR
  Store status to this database.

  Example: --database digdag

-m, --memory
  Store status in memory. Data will be removed when the server exists.

  Example: --memory

-c, --config PATH
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

-o, --output DIR
  Creates server and client configration files in this directory.

  Example: -o digdag-server



Client-mode commands
----------------------------------

Client-mode common options:

-e, --endpoint HOST
  HTTP endpoint of the server (default: 127.0.0.1:65432)

  Example: digdag-server.example.com:65432

-k, --apikey APIKEY
  Authentication API key.

  Example: -k "RqveUY_CG84/nGO8OIMlfwQu7Qzb-TRi9zP0Pif63pcHnQWCCNKXr70"

-c, --config PATH
  Configuration file path (default: ~/.digdag/client.properties).

  Example: -c digdag-server/client.properties


start
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag start <repo-name> <+name> [--now or "yyyy-MM-dd HH:mm:ss Z"]

Starts a new session. This command requires repository name, workflow name, and session_time. Examples:

.. code-block:: console

    $ digdag start myrepo +main "2016-01-01 00:00:00 -08:00"

-p, --param KEY_VALUE
  Add a session parameter (use multiple times to set many parameters) in KEY=VALUE syntax. This parameter is availabe using ``${...}`` syntax in the YAML file, or using language API.

  Example: -p environment=staging

-P, --params-file PATH
  Read parameters from a YAML file. Nested parameter (like {mysql: {user: me}}) are accessible using "." syntax (like \${mysql.user}).

  Example: -P params.yml

-R, --retry NAME
  Set attempt name to retry a session.

  -R 1


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

-r, --repository NAME
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

-i, --last-id ID
  Shows more sessions from this id

tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag tasks <session-id>

Shows tasks of a session. Examples:

.. code-block:: console

    $ digdag tasks 32


archive
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ digdag archive [-f workflow.yml...] [options...]

Creates a repository archive to upload it to the server. This command reads list of file paths to add this archive from STDIN. Examples:

.. code-block:: console

    $ git ls-files | digdag archive
    $ find . | digdag archive -o digdag.archive.tar.gz

STDIN
  Names of the files to add the archive.

-f, --file PATH
  Use this file to load tasks (default: digdag.yml)

  Example: -f tasks/another.yml

-o, --output PATH
  Output path (default: digdag.archive.tar.gz)

  Example: -o archive.tar.gz


upload
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag upload <path.tar.gz> <repository> <revision>
      Options:

Upload a repository archive to the server.


Common options
----------------------------------

-g, --log PATH
  Output log messages to a file (default is STDOUT). If this option is set, log files are rotated every 10MB, compresses it using gzip, and keeps at most 5 old files.

-l, --log-level LEVEL
  Change log level (enum: trace, debug, info, warn, or error. default is info).

-X KEY=VALUE
  Add a performance system configuration. This option is for experimental use.

