Release 0.10.4
==============

General Changes
---------------

* Logging improvements [#1680]
* Make CommandExecutor pluggable (system plugin) [#1681 #901]
* Upgrade Guice, JDBC for PostgreSQL [#1683]
* Add ``wait`` operator [#1684]
* CI improvements [#1685]
* Document improvements

CLI Changes
------------------
* Add ``--count`` and ``--last-id`` option to ``workflows`` subcommand [#1668 #1679]
* Add ``--logback-config`` option [#1682]

UI Changes
----------
* Fix an issue where the navbar collapsing does not work [#1633]
* Reduce flow errors [#1641]
* Fix an issue where the RESUME button does not work after PAUSE [#1648]
* Add support for folding task timeline on click [#1649]
* Add UI test [#1653]
* Dependency upgrades [#1689]

Fixed Issues
------------
* Add support for using SELinux with docker [#1676]

Milestone
---------
`Here <https://github.com/treasure-data/digdag/milestone/27?closed=1>`_

Release Date
------------
2022-01-19

Contributors
------------
* Emanuel Haupt
* Hiroyuki Sato
* Shota Suzuki
* djKooks
* kamikaseda
* o-mura
* seiya
* yoyama
* yuto.suzuki
