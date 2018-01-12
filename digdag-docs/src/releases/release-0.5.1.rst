Release 0.5.1
==================================


Operator changes
------------------

* ``td>`` and ``td_run`` operators support ``preview: true`` option. If it is true, it shows some rows of the query results for preview.

* ``td>`` operator adds INSERT or CREATE commands after comment lines instead of the head of the query. This is useful to keep magic comments at the header.

* ``td>`` operator adds INSERT or CREATE commands at ``-- DIGDAG_INSERT_LINE`` line if it exists in the query body. This is necessary when a Hive query includes WITH.

* ``td_ddl`` operator supports ``{create,drop,empty}_databases: [NAMES]`` options.


Release Date
------------------
2016-03-30

Contributors
------------------
* Daniel Norberg
* Sadayuki Furuhashi

