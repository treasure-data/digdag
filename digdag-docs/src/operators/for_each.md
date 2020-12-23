# for_each>: Repeat tasks for values

**for_each>** operator runs subtasks multiple times using sets of variables.

    +repeat:
      for_each>:
        fruit: [apple, orange]
        verb: [eat, throw]
      _do:
        echo>: ${verb} ${fruit}
        # this will generate 4 tasks:
        #  +for-fruit=apple&verb=eat:
        #    echo>: eat apple
        #  +for-fruit=apple&verb=throw:
        #    echo>: throw apple
        #  +for-fruit=orange&verb=eat:
        #    echo>: eat orange
        #  +for-fruit=orange&verb=throw:
        #    echo>: throw orange

## Options

* **for_each>**: VARIABLES

  Variables used for the loop in ``key: [value, value, ...]`` syntax. Variables can be an object or JSON string.

  Examples:

  ```
  for_each>: {i: [1, 2, 3]}
  ```

  Examples:

  ```
  for_each>: {i: '[1, 2, 3]'}
  ```

* **\_parallel**: BOOLEAN | OBJECT

  Runs the repeating tasks in parallel.
  If ``_parallel: {limit: N}`` (N is an integer: 1, 2, 3, …) parameter is set,
  the number of tasks running in parallel is limited to N.
  Note that the tasks in the loop will be running in serial.

  Examples:

  ```
  _parallel: true
  ```

  Examples:

  ```
  _parallel:
    limit: 2
  ```

* **\_do**: TASKS

  Tasks to run.

