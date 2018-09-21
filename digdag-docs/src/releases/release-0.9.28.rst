Release 0.9.28
==============

General Changes
---------------

* [Experimental] Divide LogView for each task on digdag-ui [#850]

* Update libraries bootstrap to 4.1.2, cryptiles to 4.1.2, macaddress to 0.2.9, and url-parse to 1.4.3 for digdag-ui [#866]

* Support `password_override` option on `pg` and `redshift` operators [#839]

* Support `num_records` to ${td.last_job} object for `td` operator [#870]

* Change `td` operator to populate secret params to result_settings [#861]

* Change workflow prefix to add task id [#834]

* Minor improve log messages shown by version command when it cannot get server version [#868]

* Update td-client to 0.8.6 for td operators [#869]

...

Release Date
------------
2018-09-21

Contributors
------------
* Hiroyuki Sato
* Kazuhiro NISHIYAMA
* Kazuhiro Sera
* Kazuhiro Serizawa
* Mitsunori Komatsu
* Muga Nishizawa
* Satoru Kamikaseda
