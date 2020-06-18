# loop>: Repeat tasks

**loop>** operator runs subtasks multiple times.

This operator exports `${i}` variable for the subtasks. Its value begins from 0. For example, if count is 3, a task runs with i=0, i=1, and i=2.

    +repeat:
      loop>: 7
      _do:
        +step1:
          echo>: ${moment(session_time).add(i, 'days')} is ${i} days later than ${session_date}
        +step2:
          echo>: ${moment(session_time).add(i, 'hours')} is ${i} hours later than ${session_local_time}.

## Options

* **loop>:** COUNT
  Number of times to run the tasks.

  Examples:

  ```
  loop>: 7
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

