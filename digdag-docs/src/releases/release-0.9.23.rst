Release 0.9.23
==============

General Changes
------------------

* Improved lock contentions for checking status of tasks on PostgreSQL. This improves performance and scalability against many active tasks.

* Notification runs outside of transaction. This makes it possible to use database transaction in a custom notification module.


Release Date
------------
2018-01-19

Contributors
------------
* Sadayuki Furuhashi
* ariarijp
