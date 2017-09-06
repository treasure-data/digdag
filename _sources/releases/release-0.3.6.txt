Release 0.3.6
==================================

General Changes
------------------

* Checks JDK version when digdag command starts. Digdag shows "too old JDK version" if JDK version is older than 8u71.
* Fixed a problem where ``${...}`` syntax was not evaluated if those parameters are included in ``_export`` or store parameters.

Operator Changes
------------------

* td, td_run td_ddl, td_load and td_table_export operators accept ``[database.]table`` syntax to specify table name as a parameter. Database name part is optional (uses default database name by default).


Release Date
------------------
2016-03-11

Contributors
------------------
* Sadayuki Furuhashi
* Taro L. Saito

