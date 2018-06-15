Release 0.9.16
==============

Operator Changes
----------------

* Fix unexpectedly escaped ``parallel`` option in ``redshift>:`` operator
* Fix unexpected retry even with ``retry: false`` option in ``http>:`` operator
* Support NULL-able values with ``pg>:`` operator's ``download_file`` option

UI Changes
---------------

* Show ``Canceled`` instead of ``Failure`` on attempt status view


General Changes
---------------

* Remove unsupported ``Accept-Encoding: deflate`` from client requests
* Avoid unefficient query plans at ``DatabaseSessionStore.getSessions`` in ``digdag-core``


Release Date
------------
2017-09-04

Contributors
------------------
* Mitsunori Komatsu
* Sadayuki Furuhashi
* You Yamagata
* grimrose

