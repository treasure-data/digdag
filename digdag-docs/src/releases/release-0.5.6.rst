Release 0.5.6
==================================

General changes
------------------

* If a local parameter is not used at a task, digdag suggests "Did you mean [<SIMILAR PARAMETER>]?" in the error message.


Local-mode changes
------------------

* ``digdag run +task`` doesn't run following tasks of ``+task``. It runs only subtree of the task.

* ``digdag run --all`` is renamed to ``digdag run --rerun``

Operator changes
------------------

* ``td>`` operator drops table through Presto using ``DROP TABLE IF EXISTS`` statement instead of issuing an REST API call so that following ``CREATE TABLE`` doesn't cause "table already exists" error because of internal cache that is not invalidated by REST API call.


Release Date
------------------
2016-04-06

Contributors
------------------
* Daniel Norberg
* Sadayuki Furuhashi

