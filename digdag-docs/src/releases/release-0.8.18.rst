Release 0.8.18
==============

Client Changes
--------------

* The ``init`` command has been fixed to work on Windows.
* The ``selfupdate`` command now properly sets the readable flag on the downloaded executable.


Workflow Changes
----------------

* The ``for_each>`` operator can now be parameterized with arrays and maps. Strings provided in the command will be parsed as JSON.
* Operators now properly clear store parameters to avoid unexpected merging of store parameters from different tasks.
* Google BigQuery operators ``bq>``, ``bq_load>``, ``bq_extract>`` and ``bq_ddl>`` operators have been added.
* A ``gcs_wait>`` operator have been added for waiting for files in Google Cloud Storage (GCS).

Release Date
------------
2016-10-31

Contributors
------------------
* Daniel Norberg
* Daniele Zannotti
* Hiroyuki Sato
* Sadayuki Furuhashi
* uu59

