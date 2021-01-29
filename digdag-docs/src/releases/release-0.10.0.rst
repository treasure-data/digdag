Release 0.10.0
==============

General Changes
---------------
* Command Executor SPI has been updated and AWS ECS Command Executor has been introduced. [#835 #1127]
* Java11 has been supported. (experimental) [#1256 #1361]
* Graaljs has been supported for ConfigEvalEngine and performance dramatically improved. [#1256 #1361]
* AccessController SPI has been introduced.  (experimental) [#936]
* Metrics improvements. (experimental) [#1146]
* Authentication configuration was changed. [#1324]
* Abolished JWT authentication. [#1324]
* Some API have been abolished and changed. [#1109 #1110 #1114 #1115 #1224 #1288 #1398 #1401]
* `_parallel` option supports a number to control number of parallels. [#1413]
* The maximum numbers of tasks and attempts are configurable. [#1430]
* Add a document `Community Contributions <https://docs.digdag.io/community_contributions.html>`_. [#1527]
* Upgrade Gradle to 6.3. [#1391]

Fixed Issues
------------
* System environment variables are not passed in except Simple Command Executor. [#1101]
* Overwriting runtime parameters has been prohibited. [#1168]
* Group retry improvements. [#1184]
* `http>` operator improvements. [#1197]
* `s3_wait>` operator improvements. [#1280]
* `py>` operator improvements. [#1477]
* TD operators improvements [#1447 #1444]
* Stability, performance improvements. [#1207 #1235 #1390 #1438 #1442 #1468 #1495]

Milestone
---------
`Here <https://github.com/treasure-data/digdag/milestone/7?closed=1>`_

Release Date
------------
2021-01-29

Contributors
------------
* You Yamagata
* Leen Sun
* Shota Suzuki
* Muga Nishizawa
* Hiroyuki Sato
* Mitsunori Komatsu
* Sadayuki Furuhashi
* Ritta Narita
* Kazuhiro Serizawa
* Edd Steel
* Daniele Zannotti
* TrsNium
* Aki Ariga
* zaimy
* yui-knk
* NARUSE, Yui
* katsuyan
* Y.Kentaro
* Trs
* Satoru Kamikaseda
* Kohki Sato
* Makoto YUI
* Pierre Delagrave
* NomadBlacky
* rubyu
* petitviolet
* NirmalaY12
* Civitaspo