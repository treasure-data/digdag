Release 0.9.18
==============

Operator Changes
------------------

* ``pg>`` and ``redshift>`` operators support ``store_last_results`` option so that following tasks can use results of a query.


UI Changes
------------------

* Added ``KILL`` button on attempt details pages.

* Fixed a problem around showing task logs that was happening when a task name contains URL special characters.

* Fixed a problem around showing task logs and workflow definitions that was happening when a server requires credentials in HTTP requests.


CLI Changes
------------------

* Fixed ``digdag secrets --set`` command so that it overwrites an existent secret string longer than the new value.


Server mode changes
---------------------

* Added ``archive.s3.path-style-access`` and ``log-server.s3.path-style-access`` options to so that project archive files and task log files can be stored on S3-compatible data stores such as Ceph, Minio, and Riak CS.


Release Date
------------
2017-10-11

Contributors
------------
* Mitsunori Komatsu
* Sadayuki Furuhashi
* Shota Suzuki
* Takahiro INOUE
* szyn

