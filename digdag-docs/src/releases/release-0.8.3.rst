Release 0.8.3
=============

General changes
---------------

* Added plugin mechanism. Now a workflow can depend on plugins released on Sonatype, Bintray, or custom maven repositories.
* Fixed Docker command executor. ``build`` option also works now.
* ``sh>`` operator supports ``shell: ARRAY`` option (@toyama0919++).


Server mode changes
-------------------

* Server supports pluggable storages to store project archives.
* Authenticator module can store login user information when a new revision is pushed. ``revisions.user_info`` column is added.


Release Date
------------
2016-06-29

Contributors
------------------
* Daniel Norberg
* Hiroyuki Sato
* Masahiro Nakagawa
* Sadayuki Furuhashi
* toyama0919

