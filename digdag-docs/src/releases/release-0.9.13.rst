Release 0.9.13
==============

CLI changes
---------------

* Added ``--max-task-threads`` to the ``run`` subcommand. When this option is set, local-mode execution limits number of max running tasks as like ``digdag server --max-task-threads`` option. (@gfx++)

* Fixed wrong time formatting when the time is past. (@akirakw++)

Operator Changes
----------------

* ``http>`` operator supports ``timeout: SECONDS`` option. (@duck8823++)


General Changes
---------------

* Let operator plugins inject CommandLogger instance. This is useful to copy log messages displayed by a subprocess to task logs. (@akirakw++)

* Added config.td.default_endpoint system configuration which sets the default td.endpoint parameter for Treasure Data operators.


UI Changes
---------------

* Improved next-run time format to include a duration until the time (@szyn++)

* Added success/warning/error messages when editing a workflow definition (@szyn++)


Release Date
------------
2017-07-18

Contributors
------------------
* Akira KAWAGUCHI
* Akira Koyasu
* FUJI Goro (gfx)
* Hiroyuki Sato
* KAWACHI Takashi
* Mitsunori Komatsu
* Sadayuki Furuhashi
* Shota Suzuki
* Shunsuke Maeda
* Takehiro Shiozaki
* Toru Takahashi
* Yoichi Nakayama
* shio-phys
* shunsuke maeda
* uu59

