Release 0.8.22
==============

General Changes
---------------

* Task and Attempt execution time limits are now enforced. The attempt is canceled and a notification sent if either limit is exceeded. The limits can be configured using the
  ``executor.attempt_ttl`` and ``executor.task_ttl`` settings. The default limits are 7 days and 1 day, respectively.

Workflow Changes
----------------

* The `rb>` and `py>` operators can now configure environment variables.

Release Date
------------
2016-12-07

Contributors
------------------
* Daniel Norberg
* Daniele Zannotti
* Sadayuki Furuhashi

