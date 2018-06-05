Release 0.9.11
==============

General Changes
---------------

* Fixed "Not in transaction" error happened with ``scheduler`` command.
* Fixed a potential problem in retrying logic that causes an exception during extracting project archive before starting a task. This happened especially when project archive is stored on S3.


Operator Changes
----------------

* ``td_table_export`` operator now supports ``file_format: jsonl.gz`` option.

Release Date
------------
2017-05-17

Contributors
------------------
* Keisuke Noda
* Mitsunori Komatsu
* Sadayuki Furuhashi
* Satoru Kamikaseda

