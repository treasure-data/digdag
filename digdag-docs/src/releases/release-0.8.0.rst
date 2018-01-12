Release 0.8.0
=============

Server mode changes
-------------------

* The server can now be configured to set custom headers on rest api HTTP response headers.
* There is now a rough initial web UI that can be used to inspect workflow status on the server.


Local mode changes
------------------

* ``digdag run`` must now be told explicitly what workflow to run.


Workflow changes
----------------

* Workflow files now use the suffix ``.dig``. The syntax is still YAML.
* The ``digdag.yml`` project file has been removed.
* The ``${session_id}`` variable has been removed.
* The `td_load>` operator now supports named Data Connector sessions.
* Workflows are now limited to a maximum of 1000 tasks.


Release Date
------------
2016-05-27

Contributors
------------------
* Daniel Norberg
* Sadayuki Furuhashi

