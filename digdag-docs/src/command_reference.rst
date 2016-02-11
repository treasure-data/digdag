Command reference
==================================

.. contents::
   :local:

Local-mode commands
----------------------------------

new
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Usage

  .. code-block:: console

      Usage: digdag new <path>

* Options

.. code-block:: console

        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config
    
      Example:
        $ digdag init mydag


run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag run [+task] [options...]
      Options:
        -f, --file PATH                  use this file to load tasks (default: digdag.yml)
        -s, --session PATH               use this directory to read and write session status
        -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)
        -P, --params-file PATH.yml       read session parameters from a YAML file
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config


check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag check [options...]
      Options:
        -f, --file PATH                  use this file to load tasks (default: digdag.yml)
        -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)
        -P, --params-file PATH.yml       read session parameters from a YAML file
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config

scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag sched [options...]
      Options:
        -f, --file PATH                  use this file to load tasks (default: digdag.yml)
        -t, --port PORT                  port number to listen for web interface and api clients (default: 65432)
        -b, --bind ADDRESS               IP address to listen HTTP clients (default: 127.0.0.1)
        -o, --database DIR               store status to this database
        -m, --memory                     uses memory database (default: true)
        -c, --config PATH.properties     server configuration property path
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config


Server-mode commands
----------------------------------

server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag server [options...]
      Options:
        -t, --port PORT                  port number to listen for web interface and api clients (default: 65432)
        -b, --bind ADDRESS               IP address to listen HTTP clients (default: 127.0.0.1)
        -o, --database DIR               store status to this database
        -m, --memory                     uses memory database
        -c, --config PATH.properties     server configuration property path
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config


genapikey
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag genapikey
      Options:
        -o, --output DIR                 creates server and client configration files
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config


Client-mode commands
----------------------------------

start
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag start <repo-name> <+name> [--now or "yyyy-MM-dd HH:mm:ss Z"]
      Options:
        -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)
        -P, --params-file PATH.yml       read session parameters from a YAML file
        -R, --retry NAME                 set attempt name to retry a session
        -e, --endpoint HOST[:PORT]       HTTP endpoint (default: 127.0.0.1:65432)
        -k, --apikey APIKEY              authentication API key
        -c, --config PATH.properties     configuration file path (default: ~/.digdag/client.properties)
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config


kill
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag kill <session-id>
      Options:
        -e, --endpoint HOST[:PORT]       HTTP endpoint (default: 127.0.0.1:65432)
        -k, --apikey APIKEY              authentication API key
        -c, --config PATH.properties     configuration file path (default: ~/.digdag/client.properties)
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config

workflows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag workflows [+name]
      Options:
        -r, --repository NAME            repository name
        -e, --endpoint HOST[:PORT]       HTTP endpoint (default: 127.0.0.1:65432)
        -k, --apikey APIKEY              authentication API key
        -c, --config PATH.properties     configuration file path (default: ~/.digdag/client.properties)
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config


schedules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag schedules
      Options:
        -e, --endpoint HOST[:PORT]       HTTP endpoint (default: 127.0.0.1:65432)
        -k, --apikey APIKEY              authentication API key
        -c, --config PATH.properties     configuration file path (default: ~/.digdag/client.properties)
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config


sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag sessions [repo-name] [+name]
      Options:
        -i, --last-id ID                 shows more sessions from this id
        -e, --endpoint HOST[:PORT]       HTTP endpoint (default: 127.0.0.1:65432)
        -k, --apikey APIKEY              authentication API key
        -c, --config PATH.properties     configuration file path (default: ~/.digdag/client.properties)
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config


tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag tasks <session-id>
      Options:
        -e, --endpoint HOST[:PORT]       HTTP endpoint (default: 127.0.0.1:65432)
        -k, --apikey APIKEY              authentication API key
        -c, --config PATH.properties     configuration file path (default: ~/.digdag/client.properties)
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config


archive
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag archive [-f workflow.yml...] [options...]
      Options:
        -f, --file PATH                  use this file to load tasks (default: digdag.yml)
        -o, --output ARCHIVE.tar.gz      output path (default: digdag.archive.tar.gz)
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config
    
      Stdin:
        Names of the files to add the archive.
  
      Examples:
        $ git ls-files | digdag archive
        $ find . | digdag archive -o digdag.archive.tar.gz


upload
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    Usage: digdag upload <path.tar.gz> <repository> <revision>
      Options:
        -e, --endpoint HOST[:PORT]       HTTP endpoint (default: 127.0.0.1:65432)
        -k, --apikey APIKEY              authentication API key
        -c, --config PATH.properties     configuration file path (default: ~/.digdag/client.properties)
        -g, --log PATH                   output log messages to a file (default: -)
        -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
        -X KEY=VALUE                     add a performance system config

