Release 0.7.1
=============

Server mode changes
-------------------

* The Digdag server now picks up mail configuration parameters from ``$HOME/.config/digdag/config``.
  The parameters have the prefix ``config.mail.`. E.g.: ``config.mail.username``, ``config.mail.password``, etc.
  These parameters are not accessible as variables in workflows executing on the server.


Workflow changes
----------------

* The variables ``last_*`` and ``next_*`` were changed to ``last_session_*``, ``next_session_*``, as specified in the docs.

Release Date
------------
2016-05-13

Contributors
------------------
* Daniel Norberg
* Sadayuki Furuhashi

