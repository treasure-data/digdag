# if>: Conditional execution

**if>** operator runs subtasks if `true` is given.

(This operator is EXPERIMENTAL. Parameters may change in a future release)

    +run_if_param_is_true:
      if>: ${param}
      _do:
        echo>: ${param} == true

## Options

* **if>**: BOOLEAN

  `true` or `false`.

* **\_do**: TASKS

  Tasks to run if `true` is given.

