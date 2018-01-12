Release 0.9.20
==============

Server Changes
------------------

* **IMPORTANT** Added ``log-server.s3.direct_download`` option. This option control whether digdag servers generate temporary pre-signed download URLs for clients to fetch log files directly from S3. Default is ``false``, where clients fetch log files through digdag server. To keep the previous behavior, you need to set it to ``true``.

* Added ``cancelRequested`` flag field to the REST API resource of tasks. This is helpful to visualize state of tasks more precisely.


UI Changes
------------------

* Fixed a regression existing in v0.9.18 and v0.9.19 where UI includes cookie in the requests of temporary pre-signed download URLs of log files and project archives. Now it doesn't include cookie. This fixes a problem where wildcard CORS header (``Access-Control-Allow-Origin: '*'``) doesn't work.

* UI visualizes blocked tasks of a canceled attempt as CANCELED state.

* UI visualizes progressing tasks excluding running tasks of a canceled attempt as CANCELING state.


Operator Changes
------------------

* ``embulk>`` operator puts both STDERR and STDOUT messages out to task logs in streaming manner.


Release Date
------------
2017-10-17

Contributors
------------
* Sadayuki Furuhashi
* hiroyukim
