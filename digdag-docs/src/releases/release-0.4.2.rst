Release 0.4.2
==================================

Server-mode changes
-------------------

* Changed /api/sessions to /api/attempts. No backward compatibility is provided.

* Client library class RestSession is renamed to RestSessionAttempt.

* Added /api/repository?name= and /api/repositories/{id}/workflow?name= REST API. This API is useful to lookup a repository or workflow by name.

* Added ``io.digdag.server.ServerBootstrap`` class. This is useful to embed digdag server to a custom application.

* Added ``DigdagEmbed.Bootstrap.overrideModulesWith`` method that allows an user application to override Guice bindings.


Release Date
------------------
2016-03-18

Contributors
------------------
* Sadayuki Furuhashi

