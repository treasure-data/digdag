Release 0.8.16
==============

Client Changes
--------------

* Added a ``--type`` flag to ``digdag init`` to select the type of project to generate.


Server Changes
--------------

* Added a new ``/api/workflows`` endpoint that can be used to list all current workflows.
* Added ``/api/schedules/{id}/disable`` and ``/api/schedules/{id}/enable`` endpoints that can be used to disable and re-enable scheduled execution of a workflow.
* Added ``/api/schedules`` and ``/api/projects/{id}/schedules`` endpoint that can be used to list all schedules and the schedules of a project.
* The number of active attempts per site is now limited. The default limit is ``100`` and it an be controlled using the ``io.digdag.limits.maxAttempts`` system property.


Workflow Changes
----------------

* Added an ``s3_wait>`` operator that can be used to wait for a file to come into existence in AWS S3.
* The ``mail>`` operator now requires the ``password`` to be provided as a secret.
* The ``td_table_export`` operator now requires S3 credentials to be provided as secrets.

Release Date
------------
2016-09-29

Contributors
------------------
* Daniel Norberg
* Daniele Zannotti
* Mitsunori Komatsu
* Sadayuki Furuhashi

