Release 0.4.3
==================================

General changes
------------------

* Upgraded JDBI from 2.63.1 to 2.72. `Release Notes <https://github.com/jdbi/jdbi/blob/5bef8b287d09ce82dcc76563a9e5134f98cd3892/RELEASE_NOTES>`_.

* Upgraded HikariCP from 2.4.3 to 2.4.5. `Changes <https://github.com/brettwooldridge/HikariCP/blob/b4c358a4c3c003a9ffbb0ecfe7685dd07cbd759f/CHANGES>`_.

* Validation error is throw if workflow definition includes more than 1 operators in a task.

* YAML loader detects duplicated keys in a single mapping object and throws an exception.


CLI changes
------------------

* Added ``backfill``, ``reschedule``, and ``log`` to usage message.

Client-mode changes
-------------------

* Added ``DigdagClient.getRevisions``.

Server-mode changes
-------------------

* Added ``/api/repositories/{id}/revisions`` REST API.

* OperatorManager includes error message in task logs such as no-such-operator error.

* Fixed permission error when uuid-ossp module is already loaded on PostgreSQL.

* Fixed putAndLockRepository on PostgreSQL.

* insertAttempt validates existence of associated workflow definition and using foreign key validation.


Release Date
------------------
2016-03-28

Contributors
------------------
* Daniel Norberg
* Hiroshi Nakamura
* Sadayuki Furuhashi

