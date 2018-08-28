Release 0.8.19
==============

Server Changes
--------------

* Added an ``isAdmin`` flag to the ``Authenticator.Result`` interface that can be used to grant users administrator rights.
* Added an administration API with an endpoint that can be used by administrators to get ``userinfo`` metadata for a session attempt.

UI Changes
----------

* Added a button for retrying failed attempts.
* Added a button for pausing scheduled workflow execution.

Workflow Changes
----------------

* The ``gcs_wait>`` operator now allows ``gs://`` URI's on the operator command line.
* Added an ``http>`` operator that can be used to make HTTP requests.
* The ``for_each`` operator now generates subtasks with shorter names.
* Fixed an issue where the ``td_table_export>`` operator was not able to use ``aws.s3.*`` secrets.
* The ``s3_wait>`` operator will now not retry ``4xx` HTTP errors except for ``429 Too Many Requests`` and ``408 Request Timeout``.
* Fixed an issue where the ``td>`` operator ``download_file`` option would sometimes download a corrupt file.

Release Date
------------
2016-11-15

Contributors
------------------
* Daniel Norberg
* Daniele Zannotti
* Tony Wei

