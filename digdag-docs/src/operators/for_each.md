# for_each>: Repeat tasks

**for_each>** operator runs subtasks multiple times using sets of variables.

(This operator is EXPERIMENTAL. Parameters may change in a future release)

    +repeat:
      for_each>:
        fruit: [apple, orange]
        verb: [eat, throw]
      _do:
        sh>: echo ${verb} ${fruit}
        # this will generate 4 tasks:
        #  +for-fruit=apple&verb=eat:
        #    sh>: echo eat apple
        #  +for-fruit=apple&verb=throw:
        #    sh>: echo throw apple
        #  +for-fruit=orange&verb=eat:
        #    sh>: echo eat orange
        #  +for-fruit=orange&verb=throw:
        #    sh>: echo throw orange

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

* **\_parallel**: BOOLEAN

  Runs the repeating tasks in parallel.

  Examples:

  ```
  _parallel: true
  ```

* **\_do**: TASKS

  Tasks to run.

