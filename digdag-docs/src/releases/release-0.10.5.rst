Release 0.10.5
==============

General Changes
---------------
* API improvements [#1697 #1720]
* Build/CI improvements [#1733 #1735 #1739 #1740 #1770 #1782 #1783]
* Documentation improvements [#1708 #1730 #1744 #1746 #1768]
* Logging/TD related improvements [#1751 #1765]
* Retry if plugin fails to load [#1728]
* Support for ``ecs.<name>.use_environment_file`` config on EcsCommandExecutor [#1762]
* Support for ``location`` option on ``bq`` operator [#1762]
* Support for ``retry`` command to overwrite parameter [#1725]
* Support for ``ssl`` related config for RemoteDatabaseConfig [#1690]
* Support for CLI options regarding API improvements [#1778]
* Support for start_date and end_date for schedule [#1750]
* Upgrade Gradle to 6.9.2 [#1754]
* Upgrade kubernetes-client version to 5.12.1 [#1722]

Fixed Issues
------------
* EcsCommandExecutor: Retry ECS RunTask on AGENT error [#1723]
* EcsCommandExecutor: Return failure status if exit code is empty [#1724]
* GCSStorage: NPE issue [#1753]
* KubernetesCommandExecutor: Delete k8s pods when finished [#1696]
* UI: Workflow page memory consumption of browser [#1652 #1741]

UI Changes
----------
* Dependency upgrades [#1707 #1715 #1731 #1732 #1743 #1746 #1757 #1761 #1773 #1774 #1775 #1776 #1777]
* Migration to Typescript [#1698]
* Support for copying logs to clipboard [#1760]
* Support for showing detailed time on mouse over relative time [#1719]

Milestone
---------
`See <https://github.com/treasure-data/digdag/milestone/28?closed=1>`_

Release Date
------------
2023-02-14

Contributors
------------
* Akito Kasai
* Emanuel Haupt
* Jungwon Choi
* KAWACHI Takashi
* Kazuhiro Serizawa
* KentFujii
* Kyoppii
* Mikio Tachibana
* Naoto Yokoyama
* RyoAriyama
* Seiya
* Shota Suzuki
* Shuhei Nagasawa
* You Yamagata
* dependabot[bot]
* gumigumi4f
* hideki.narimiya
* kohki1234
* kyoppii13
* rune-chan
* ryo.ariyama
* yoyama
