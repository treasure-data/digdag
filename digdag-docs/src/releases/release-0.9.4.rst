Release 0.9.4
=============

CLI Changes
---------------

* Added local secrets. New ``digdag secret --local`` command lets you to set secrets for local mode.
* Version checking become less restricted. CLI checks there is a recommended upgrade or not using Digdag server's REST API and upgrades only if one exists. If CLI is executed as a batch command (STDOUT is not TTY), force upgrade won't be required.

Operator Changes
----------------

* ``td_run`` operator accepts ID of a bulk loading in addition to name.

UI Changes
---------------

* Web UI has a new interface that creates a new project or edits workflow definitions.

General Changes
---------------

* S3 storage extension is now bundled with the released package. Digdag server uses S3 as the project archive storage if ``archive.type = s3`` and ``archive.s3.bucket`` options are set, and uses S3 as the log file storage if ``log-server.type = s3`` and ``log-server.s3.bucket`` options are set.

Release Date
------------
2017-02-06

Contributors
------------------
* Daniel Norberg
* Daniele Zannotti
* Kazuki Ohta
* Mitsunori Komatsu
* Sadayuki Furuhashi

