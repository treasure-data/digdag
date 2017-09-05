Release 0.7.0
=============

CLI changes
-----------

* Client-mode commands now verify that the server and cli version match and prompts the user to run ``digdag selfupdate`` if the cli is out of date. This check can be disabled using the ``--disable-version-check`` flag.

* ``digdag attempts <id>`` can now be used to show a single workflow session attempt.

* ``digdag push -c <config file>`` now correctly uses the specified configuration while when creating the project archive.

* ``digdag push`` now creates the project archive in a the system temp directory instead of in the current working directory.

* Default digdag configuration location has been moved from ``$HOME/.digdag/config`` to ``$HOME/.config/digdag/config``. Digdag now also respects the ``XDG_CONFIG_HOME`` environment variable for specifying a different configuration location. See: https://standards.freedesktop.org/basedir-spec/latest/index.html

Server mode changes
-------------------

* Json access log now includes request time.

* The Digdag server now picks up parameters from ``$HOME/.config/digdag/config``. All workflows executed by the server can make use of these parameters. This can be useful to e.g. have a server side SMTP configuration.

Release Date
------------
2016-05-12

Contributors
------------------
* Daniel Norberg
* Sadayuki Furuhashi

