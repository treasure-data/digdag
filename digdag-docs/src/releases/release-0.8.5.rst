Release 0.8.5
=============

General changes
---------------

* Fixed ``file name ... is too long ( > 100 bytes)`` error that happened when ``digdag push`` command archives file names longer than 100 bytes. Now digdag client and server use PAX extension to handle long file names.


Server mode changes
-------------------

* Changed ``/api/logs/{attempt_id}/files`` REST API to return direct download URL without wrappnig in an object. This is a backward incompatible change.

* Digdag server waits for completion of running tasks when it exits.


Release Date
------------
2016-07-26

Contributors
------------------
* Daniel Norberg
* Sadayuki Furuhashi

