Release 0.9.3
=============

Server mode changes
-------------------

* Project archives can now be stored in S3 instead of the database. This is configured using the ``archive.s3.bucket``, ``archive.s3.credentials.access-key-id`` and ``archive.s3.credentials.secret-access-key`` configuration parameters.

Workflow changes
----------------

* The ``emr>`` operator now uses ``${secret:<key>}`` syntax.
* The ``http>`` operator now allows secrets in headers, content and uri using ``${secret:<key>}`` syntax.
* Declaring secret access using ``_secrets`` is no longer necessary and has been deprecated.
* It is now possible to use secrets in ``td_load>`` and ``embulk>`` configuration using ``${secret:<key>}`` syntax.
* The ``td_run>`` operator can now run a saved query by id. If the operator command is a number, the query with that id will be run.
* The ``td_ddl>`` operator now allows entire parameter lists to be parameterized.

General Changes
---------------

* Secret access limits and policies have been removed. Operators can now access any secret.
* Backfills now run sequentially instead of all sessions in parallel.
* The Digdag client now sends a ``User-Agent`` header that includes the build artifact version.
* Project metadata compilation has been moved to the server. The project tarball no longer needs to include a ``.digdag.dig`` file when pushing.

Release Date
------------
2017-01-17


Contributors
------------------
* Daniel Norberg
* Mitsunori Komatsu
* Sadayuki Furuhashi

