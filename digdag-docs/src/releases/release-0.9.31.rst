Release 0.9.31
==============

General Changes
---------------

* Introduce `param_set>` and `param_get>` operators, which enables storing parameters to external data store [#845]

* Introduce `_else_do` subtasks in `if>` operator [#617]

* Introduce `page_size` query parameter for `GET /api/attempts` and `GET /api/sessions`. 100 per page by default. change with api.max_{attempts,sessions}_page_size as well [#880]

* Introduce `python` option in `py>` operator to customize python executable command [#703, #890]


Release Date
------------
2018-10-05

Contributors
------------
* Kazuhiro Serizawa
* Muga Nishizawa
* You Yamagata
* george
