Release 0.5.0
==================================

CLI changes
------------------

* Setting parameters in command line (``-p KEY=VALUE``) accepts dot-separated keys to set a nested value. For example, ``-p td.apikey=value`` sets ``{td: {apikey: value}}``.

Client-mode changes
-------------------

* ``start`` command requires ``--session`` argument instead of ``--now`` or zoned timestamp.

* ``start`` command accepts ``--revision <name>`` option to run a workflow using a past revision.

* Added ``retry`` command.

* ``workflows`` command accepts repository name as an extra command line argument instead of -r, --repository option.

* ``backfill`` command no longer accepts ``-R`` as an alias of ``--attempt-name``.

* Added ``attempts`` command. ``session`` command no longer accepts ``--with-retry`` (use ``attempts`` command instead).

Server-mode changes
-------------------

* ``PUT /api/attempts`` endpoint requires workflow id instead of a combination of repository name, revision name, and workflow name. This makes the request idempotent and deterministic.

* Added ``GET /api/workflows/{id}/truncated_session_time`` to calculate a session time using the workflow's time zone. This API is useful to prepare a new session attempt.

* Added ``/api/workflow?repository=<name>&name=<name>[&revision=<name>]`` endpoint. This API is useful to lookup a workflow by name.

* Added ``GET /api/workflows/{id}`` endpoint.

* ``/api/repository`` and ``/api/repositories/{id}`` endpoints no longer accept ``?revision=<name>`` parameter. These endpoints return always repositories with the last revision name.

* ``/api/schedules``, ``/api/schedules/{id}``, and ``/api/schedules/{id}/skip`` endpoints return workflow id in addition to workflow name.

* ``/api/attempts``, ``/api/attempts/{id}``, ``/api/attempts/{id}/retries``, ``/api/schedules/{id}/backfill``, ``/api/repositories/{id}/workflow`` and ``/api/repositories/{id}/workflows`` endpoints return a session attempt with optional workflow id in addition to workflow name.

* ``/api/schedules``, ``/api/schedules/{id}``, and ``/api/schedules/{id}/skip`` endpoints return nextRunTime in ISO timestamp format in UTC instead of UNIX timestamp, and nextScheduleTime in ISO timestamp format with zone offset.

* ``/api/schedules/{id}/backfill`` endpoint requires fromTime parameter in ISO timestamp format with zone offset instead of UNIX timestamp.

* ``/api/schedules/{id}/skip`` endpoint requires fromTime and nextRunTime in ISO timestamp format with zone offset instead of UNIX timestamp. It requires nextTime in ISO timestamp with or without zone offset instead of UNIX timestamp.


General changes
------------------

* Added ``workflow_configs.timezone`` column on database. This is not backward comptible if the data is persistent in H2 database file or PostgreSQL. Migration script is not available at least at this point.


Release Date
------------------
2016-03-30

Contributors
------------------
* Sadayuki Furuhashi

