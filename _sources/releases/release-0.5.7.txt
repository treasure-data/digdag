Release 0.5.7
==================================

Server-mode changes
-------------------

* Workspace extraced from an repository archive preserves UNIX file permissions and symbolic links.

* Added retrying to all internal database operations.


Client-mode changes
-------------------

* ``digdag push`` and ``digdag archive`` preserves UNIX file permissions and symbolic linkes.

* ``digdag start`` supports ``-d, --dry-run``.

* Fixed wrong session time truncation of ``didgag start --session`` option.


Release Date
------------------
2016-04-06

Contributors
------------------
* Sadayuki Furuhashi

