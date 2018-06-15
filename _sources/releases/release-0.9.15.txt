Release 0.9.15
==============

UI Changes
---------------

* Improved order of tasks in attempt pages when tasks are generated dynamically. Misunderstanding order of tasks was happening with if>, loop>, for_each>, and other task-generating tasks.


General Changes
---------------

* Failed tasks keep state_params stored in the backend database (@akirakw++). This is for debugging purpose so that we can investigate the reasons why tasks failed using state_params.


Release Date
------------
2017-08-17

Contributors
------------------
* Akira KAWAGUCHI
* Mitsunori Komatsu
* Sadayuki Furuhashi

