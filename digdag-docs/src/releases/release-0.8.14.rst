Release 0.8.14
==============

Server Changes
--------------

* Workflow schedules are now properly recalculated when projects are pushed.
* Uncaught error reporting can now be customized. Default implementation reports counts using JMX.
* Secret access is now allowed by default if no secret policy is configured.


Workflow Changes
----------------

* The ``td>`` operator now correctly handles empty query results when using ``store_last_results: true``.

Release Date
------------
2016-09-14

Contributors
------------------
* Daniel Norberg
* Daniele Zannotti
* Mitsunori Komatsu
* Sadayuki Furuhashi

