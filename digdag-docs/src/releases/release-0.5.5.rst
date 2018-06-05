Release 0.5.5
==================================

Client-mode changes
-------------------

* ``digdag log`` tries to download task log files directly from a shared remote storage without accessing to a digdag server if the server sets direct download URL.

Server-mode changes
-------------------

* Parameter name ``log-server.path`` is changed to ``log-server.local.path``.

* Supports task logging to a remote storage.

* Added S3 task log server implementation so that application can inject it.

General changes
------------------

* Fixed broken task logging when log message includes control sequence or multibyte characters.

SPI changes
------------------

* ``LogServerFactory.getLogServer`` method doesn't receive systemConfig any more. Implementations should require one at a constructor so that it gets injected.


Release Date
------------------
2016-04-01

Contributors
------------------
* Sadayuki Furuhashi

