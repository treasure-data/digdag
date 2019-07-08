Release 0.9.38
==============

General Changes
---------------
* Introduce `max_response_content_size` system config for `http_call>` [#1138]
* Introduce `content_type_override` parameter in `http_call` [#1166]
* Improve error message and stacktrace on `py>` [#1143]
* Upgrade Sphinx version to 2.1.1 and now using python3 for document building. [#1154]


Fixed Issues
------------
* Remove useless migration execution in CLI [#1137]
* Fix an issue on `bq_load>` [#1141]
* Stop using a deprecated module in `py>` to suppress warning message. [#1162]
* Document improvements.
* Build/CI improvements.
* Update some packages for security vulnerabilities.



Release Date
------------
2019-07-09

Contributors
------------
* Hiroyuki Sato
* John Yani
* rubyu
* Aki Ariga
* Edd Steel
* Muga Nishizawa
* Sadayuki Furuhashi
* Satoru Kamikaseda
* Shota Suzuki
* Timothée Peignier
* yui-knk
* You Yamagata
