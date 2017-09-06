Release 0.8.10
==============

Server mode changes
-------------------

* Added a ``server.jmx.port`` option to start JMX agent.
* Server now waits for running tasks to complete when shutting down.
* Fixed a ui bug where logs for projects containing a legacy ``digdag.yml`` file would not be displayed.

General changes
---------------

* Added a ``finished_at`` field to session attempts.
* Fixed a symlink handling bug in project archives.
* ``td_partial_delete>`` operator now uses domain keys.
* Fixed a ``td>`` operator ``store_last_results`` bug.

Release Date
------------
2016-08-23

Contributors
------------------
* Daniel Norberg
* Mitsunori Komatsu
* Sadayuki Furuhashi
* kamikaseda

