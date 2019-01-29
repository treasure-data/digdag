Release 0.9.32
==============

General Changes
---------------

* Add CLI option --enable-swagger for REST API document [#577, #906]

* --task-log option in local mode [#784]

* Added 'attempt_id' to the RuntimeParams [#900]

* Support type hints for Python3 on py> operator [#905]

* Show warn message when tasks failed by max task limit in WorkflowExecutor [#951]

* Retry works in some generated tasks (e.g. call, loop) [#928]

* Upgrade packages [#909, #940, #953]

* Some document improvements


Fixed Issues
------------

* Some security vulnerabilities [#895, #896]

* Group retry does not work in call> operator [#928]

* Fix default argument check on py> operator [#913]



Release Date
------------
2019-01-29

Contributors
------------
* Aki Ariga
* Akira Koyasu
* Kazuhiro Serizawa
* Mitsunori Komatsu
* Muga Nishizawa
* Shota Suzuki
* You Yamagata
* george
