Release 0.9.17
==============

Operator Changes
------------------

* Support json table schema (``schema:`` option) for ``bq_load>`` operator. See operator documents for details.


General Changes
---------------

* Fixed task execution failure in server-mode when project archive contains a symlink. Now server skips setting UNIX permission if extracted file is a symlink.

* Fixed priority order of stored parameters in server-mode. See #653 for details.

* Removed false "_background (_after) is not used" warning message.

* Fixed warning message syntax for setting _background to parallel grouping tasks and setting _after option to non-parallel grouping tasks.


Release Date
------------
2017-09-28

Contributors
------------------
* Hiroyuki Sato
* Keisuke Noda
* Mitsunori Komatsu
* Ryo Okubo
* Sadayuki Furuhashi
* syucream

