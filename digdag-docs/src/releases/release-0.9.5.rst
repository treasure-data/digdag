Release 0.9.5
=============

General Changes
---------------

* Added ``${session_id}`` built-in variable.
* Fixed ``_workdir`` variable in a workflow called through another workflow. This fixes "no such file" error when a task reads a file and the task is in a workflow nested for 2 or more levels.
* Added ``timezone`` field in the return body of ``/api/workflows``.
* ``/api/workflows`` accepts ``?count=`` parameter to override the default value of 100.


Release Date
------------
2017-02-16

Contributors
------------------
* Michihito Shigemura
* Mitsunori Komatsu
* Sadayuki Furuhashi

