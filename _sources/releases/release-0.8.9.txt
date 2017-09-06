Release 0.8.9
=============

Client mode changes
-------------------

* ``start`` command now hints about ``retry`` command when trying to start an already existing session.
* CLI now respects ``https_proxy``, ``HTTPS_PROXY``, ``http_proxy`` and ``HTTP_PROXY`` environment variables.
* Added a new ``secrets`` command that an be used to set, list and delete per-project secrets.


Server mode changes
-------------------

* Starting an attempt of a workflow with more than 1000 tasks now fails with ``400 Bad Request`` instead of ``500 Internal Error``.


General changes
---------------

* Added ``td.proxy.use_ssl`` parameter to support HTTPS forward proxies.
* ``td_partial_delete>`` operator now accepts raw epoch integers in ``from:`` and ``to:`` parameters.
* ``td`` operators now respects ``https_proxy``, ``HTTPS_PROXY``, ``http_proxy`` and ``HTTP_PROXY`` environment variables.
* Added a secret management mechanism that can be used to securely provide operators with credentials such as api keys, user names and passwords.


Release Date
------------
2016-08-18

Contributors
------------------
* Daniel Norberg
* Kohei Nozaki
* Mitsunori Komatsu
* Sadayuki Furuhashi
* Y.Kentaro
* kiyoto

