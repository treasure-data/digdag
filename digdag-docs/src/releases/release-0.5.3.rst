Release 0.5.3
==================================

Client-mode changes
-------------------

* All commands support ``-H, --header KEY=VALUE`` option to add a custom HTTP header.

* client.properties can include ``header.<KEY> = <VALUE>`` lines where ``<KEY>`` is HTTP header name and ``<VALUE>`` is its value.

Server-mode changes
-------------------

* Added ``/api/version`` endpoint.

* Added ``Authenticator`` interface so that applications can overwrite authentication mechanism.

* Replaced ``ServerModule.bindAuthInterceptor`` with ``bindAuthenticator`` which improves type-safety.


Release Date
------------------
2016-03-31

Contributors
------------------
* Mitsunori Komatsu
* Sadayuki Furuhashi

