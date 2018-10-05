# if>: Conditional execution

**if>** operator runs subtasks if `true` is given.

(This operator is EXPERIMENTAL. Parameters may change in a future release)

    +run_if_param_is_true:
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
