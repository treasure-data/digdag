Release 0.9.25
==============

General Changes
---------------

* Upgraded Jackson from 2.6.7 to 2.8.11 because of `CVE-2016-7051 <https://www.cvedetails.com/cve/CVE-2016-7051/>`_ [#733].

* Upgraded moment.js from 2.15.0 to 2.21.0 because of `CVE-2017-18214 <https://nvd.nist.gov/vuln/detail/CVE-2017-18214>`_. See the change log of moment.js `here <https://github.com/moment/moment/blob/develop/CHANGELOG.md#2220-see-full-changelog>`_ [#742, #761].

* Removed Files.setPosixFilePermissions method call for Windows. [#765]

* Implemented disable_backfill. [#752]

* Upgraded node-uuid to uuid. [#744]

* Added dependencies for copied tasks for grouping task retry including nested tasks [#738]

* Randomized iteration across multiple sites over the shared queue [#737]

Release Date
------------
2018-05-25

Contributors
------------
* Daniele Zannotti
* Hiroyuki Sato
* Mitsunori Komatsu
* Muga Nishizawa
* Satoshi Ogasawara
* Sadayuki Furuhashi
* Shota Suzuki
* Toru Takahashi
* Yukihiro Okada
