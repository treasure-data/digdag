# if>: Conditional execution

**if>** operator runs `_do` subtasks if `true` is given.

    +run_if_param_is_true:
      if>: ${param}
      _do:
        echo>: ${param} == true

`_else_do` subtasks are executed if `false` is given as the operator's condition..

    +run_if_param_is_false:
      if>: ${param}
      _do:
        echo>: ${param} == true
      _else_do:
        echo>: ${param} == false

## Options

* **if>**: BOOLEAN

  `true` or `false`.

* **\_do**: TASKS

  Tasks to run if `true` is given.

* **\_else_do**: TASKS

  Tasks to run if `false` is given.

Note: _do or _else_do must be specified.
